//! A tokio-based, rust only, self baked, partial rsync wire protocol implementation for retrieving
//! files from rsyncd servers.
//!
//! Quick example:
//! ```
//! // Print all the Manifest files for gentoo ebuilds
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let url = url::Url::parse("rsync://rsync.de.gentoo.org/gentoo-portage/")?;
//!     let (mut client, files) = arrsync::RsyncClient::connect(&url).await?;
//!     for file in files.into_iter() {
//!         if !file.is_file() {
//!             continue;
//!         }
//!         if !file.path.ends_with("/Manifest".as_bytes()) {
//!             continue;
//!         }
//!         async fn get_and_print(
//!             file: arrsync::File,
//!             client: arrsync::RsyncClient
//!         ) -> anyhow::Result<()> {
//!             use tokio::io::AsyncReadExt;
//!             let mut content = vec![];
//!             client.get(&file).await?.read_to_end(&mut content).await?;
//!             // "Race" to finish. Ignored for simplicity reasons.
//!             print!("{}", String::from_utf8(content)?);
//!             Ok(())
//!         }
//!         tokio::spawn(get_and_print(file, client.clone()));
//!     }
//!     client.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! Beware that rsync is, of course, unencrypted.

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use core::task::Poll;
use std::marker::Unpin;
use std::path::Path;
use std::pin::Pin;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncBufRead, AsyncRead, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
fn default<T: Default>() -> T {
	std::default::Default::default()
} // default_free_fn
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrder};
use std::sync::{Arc, Mutex as SyncMutex};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
/// The main client struct
pub struct RsyncClient {
	inner: Arc<RsyncClientInner>,
}

struct RsyncClientInner {
	reqs: Requests,
	write: AsyncMutex<OwnedWriteHalf>,
	finish: SyncMutex<Option<tokio::task::JoinHandle<Result<Enveloped>>>>,
	id: usize,
}

#[derive(Clone, Debug)]
/// Connection statistics provided by server
pub struct Stats {
	/// Bytes sent by us
	pub bytes_read: i64,
	/// Bytes sent by the server/sender
	pub bytes_written: i64,
	/// Total size of files on the server.
	/// (Since this client knows nothing of exclusion lists, you can also calculate this from the file list.)
	pub file_size: i64,
}

impl RsyncClient {
	/// Open a connection to an rsync server and read the initial file list.
	/// The url must have scheme `rsync` and contain at least one path element (module listing is
	/// not supported).
	pub async fn connect(url: &url::Url) -> Result<(RsyncClient, Vec<File>)> {
		let (path, base) = Self::parse_url(&url)?;
		let (read, mut write) = Self::stream(&url).await?;
		let mut read = BufReader::new(read);
		Self::send_handshake(&mut write, path, base).await?;
		Self::read_handshake(&mut read, &url.origin().unicode_serialization()).await?;
		let mut read = EnvelopeRead::new(read); // Server multiplex start
		let id = NEXT_CLIENT_ID.fetch_add(1, AtomicOrder::SeqCst);
		let files = Self::read_file_list(&mut read, id).await?;
		write.write_i32_le(-1).await?;
		anyhow::ensure!(
			read.read_i32_le().await? == -1,
			"Phase switch receive-list -> receive-file"
		);
		let reqs: Requests = Arc::new(SyncMutex::new(Some(default())));
		let process = tokio::spawn(
			ReadFilesProcess {
				read,
				reqs: reqs.clone(),
				current: None,
			}
			.run(),
		);
		let inner = RsyncClientInner {
			reqs: reqs.clone(),
			write: AsyncMutex::new(write),
			finish: SyncMutex::new(Some(process)),
			id,
		};
		Ok((RsyncClient { inner: Arc::new(inner) }, files))
	}
	/// Requests the transfer of a [File](crate::File).
	/// The referenced File must have been returned in the same call as `self`, or an error will be
	/// returned.
	///
	/// There are some worrying remarks in the rsync protocol documentation in openrsync.
	/// It is stated that requested files may be silently ommited from transfer,
	/// and that files are not necessarily transmited in the order they were requested.
	/// Effectively, this means that the only way to detect that a file wasn't sent is to invoke
	/// [close](crate::RsyncClient::close), and then to wait for the sender to signal the end of the
	/// connection. Without calling close, the returned [AsyncRead](crate::AsyncRead) may remain
	/// pending forever.
	pub async fn get(&self, file: &File) -> Result<impl AsyncRead> {
		anyhow::ensure!(
			self.inner.id == file.client_id,
			"Requested file from client that was not listed in that client's connect call"
		);
		anyhow::ensure!(file.is_file(), "Can only request files, {} is not", file);
		let (data_send, data_recv) = mpsc::channel(10);
		self.inner
			.reqs
			.lock()
			.unwrap()
			.as_mut()
			.context("Client fully exited")?
			.entry(file.idx)
			.or_insert(vec![])
			.push(data_send);
		let mut write = self.inner.write.lock().await;
		log::debug!("Requesting index {}: {}", file.idx, file);
		write.write_i32_le(file.idx as i32).await?;
		write.write_all(&[0u8; 16]).await?; // We want no blocks, blocklength, checksum, or terminal block. 4 x 0i32 = 16
		Ok(tokio::io::stream_reader(data_recv))
	}
	/// Finalizes the connection and returns statistics about the transfer (See [Stats](crate::Stats)).
	/// All files that have allready been requested will be transferred before the returned future completes.
	/// Only one call to this function can succeed per [RsyncClient](crate::RsyncClient).
	pub async fn close(&mut self) -> Result<Stats> {
		let read = self.inner.finish.lock().unwrap().take();
		if let Some(read) = read {
			let mut write = self.inner.write.lock().await;
			write.write_i32_le(-1).await?; // phase switch, read by ReadProcess
			let mut read = read.await??;
			let stats = Stats {
				bytes_read: read.read_rsync_long().await?,
				bytes_written: read.read_rsync_long().await?,
				file_size: read.read_rsync_long().await?,
			};
			log::debug!("Rsync Stats: {:?}", stats);
			write.write_i32_le(-1).await?; // EoS
			write.shutdown();
			Ok(stats)
		} else {
			anyhow::bail!("Only one can close");
		}
	}

	fn parse_url(url: &url::Url) -> Result<(&Path, &str)> {
		anyhow::ensure!(url.scheme() == "rsync", "Only rsync urls supported, not {}", url);
		anyhow::ensure!(url.path() != "", "Path cannot be / - your url {} is cut short.", url);
		let path = Path::new(url.path());
		let path = if path.is_absolute() {
			path.strip_prefix(Path::new("/"))
				.expect(&format!("Stripping root from path {:?}", path))
		// Does this work on windows?
		} else {
			path
		};
		let mut base = path;
		let base = loop {
			match base.parent() {
				Some(p) if p == Path::new("") => break base,
				None => panic!("Getting base element of path {:?}",),
				Some(p) => base = p,
			};
		};
		let base = base.to_str().expect("TODO: handle paths properly");
		Ok((path, base))
	}
	async fn stream(url: &url::Url) -> Result<(OwnedReadHalf, OwnedWriteHalf)> {
		let mut addrs = url
			.socket_addrs(|| Some(873))
			.context(format!("Get socket addr from url {}", url))?;
		let mut stream =
			TcpStream::connect(addrs.pop().expect("Successful resolution returns at least one address")).await;
		for addr in addrs.iter() {
			stream = TcpStream::connect(addr).await;
			if stream.is_ok() {
				break;
			}
		}
		Ok(stream
			.context(format!("Connect to {} ({:?})", url, addrs))?
			.into_split())
	}
	async fn send_handshake(write: &mut OwnedWriteHalf, path: &Path, base: &str) -> Result<()> {
		// rsync seems to be ok with us sending it all at once
		let initial: Vec<u8> = [
			// TODO: The openrsync docs only exist for rsync 27, but below rsync 30, dates beyond 1970 + 2^31 can't be handled.
			// Client hello
			&b"@RSYNCD: 27.0\n"[..],
			// Select root
			base.as_bytes(),
			&b"\n"[..],
			// Remote command
			&b"--server\n"[..],
			&b"--sender\n"[..],
			&b"-rl\n"[..],
			&b".\n"[..],
			path.to_str().unwrap().as_bytes(),
			&b"/\n\n"[..],
			// Exclusion list
			&b"\0\0\0\0"[..],
		]
		.concat();
		write.write_all(&initial).await?;
		Ok(())
	}
	async fn read_handshake<T: AsyncBufRead + Unpin>(read: &mut T, origin: &str) -> Result<()> {
		let hello = &mut String::new();
		read.read_line(hello).await.context("Read server hello")?;
		let rsyncd = "@RSYNCD: ";
		anyhow::ensure!(
			hello.starts_with(rsyncd),
			"Not an rsync server? First message was {}",
			hello
		);
		let ver = hello[rsyncd.len()..(hello.len() - 1)]
			.split('.')
			.map(str::parse)
			.collect::<Result<Vec<u64>, _>>()
			.context(format!("Parsing server version from {}", hello))?;
		anyhow::ensure!(
			ver >= vec![27, 0],
			"Server version {} not supported - need 27.0 minimum.",
			ver.iter().map(|i| format!("{}", i)).collect::<Vec<_>>().join(".")
		);
		let mut motd = String::new();
		loop {
			let select = &mut String::new();
			read.read_line(select).await.context("Read server startup")?;
			if select == &format!("{}OK\n", rsyncd) {
				break;
			} else {
				motd += select;
			}
		}
		if &motd != "" {
			log::info!("MOTD from {}\n{}", origin, motd.strip_suffix("\n").unwrap_or(&motd));
		}
		let _random_seed = read.read_u32().await?; // Read random seed - we don't care
		Ok(())
	}
	async fn read_file_list<T: AsyncRead + Unpin + Send>(read: &mut T, client_id: usize) -> Result<Vec<File>> {
		let mut ret = vec![];
		let mut filename_buf: Vec<u8> = vec![];
		let mut mode_buf = None;
		let mut mtime_buf = None;
		loop {
			let meta = FileEntryStatus(read.read_u8().await?);
			if meta.is_end() {
				break;
			}
			let inherited_filename_length = if meta.inherits_filename() {
				read.read_u8().await? as usize
			} else {
				0
			};
			anyhow::ensure!(
				inherited_filename_length <= filename_buf.len(),
				"Protocol error: file list format inconsistency"
			);
			let filename_length = if meta.integer_filename_len() {
				read.read_u32_le().await? as usize
			} else {
				read.read_u8().await? as usize
			};
			filename_buf.resize(filename_length + inherited_filename_length, 0);
			read.read_exact(&mut filename_buf[inherited_filename_length..]).await?;
			let show = String::from_utf8_lossy(&filename_buf);
			let size = read.read_rsync_long().await?;
			anyhow::ensure!(size >= 0, "Protocol error: negative file size for {}", show);
			let size = size as u64;
			mtime_buf = if meta.mtime_repeated() {
				mtime_buf
			} else {
				let ts = read.read_i32_le().await?;
				let naive = NaiveDateTime::from_timestamp(ts as i64, 0);
				let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
				Some(datetime)
			};
			let mode = if meta.file_mode_repeated() {
				mode_buf.context(format!("Protocol error: first file {} without mode", show))?
			} else {
				read.read_u32_le().await?
			};
			mode_buf = Some(mode);
			if !meta.uid_repeated() {
				// Actually, I expect the uids to be never present. Oh well.
				let _uid = read.read_i32_le().await?;
			}
			if !meta.gid_repeated() {
				let _gid = read.read_i32_le().await?;
			}
			let symlink = if unix_mode::is_symlink(mode) {
				let len = read.read_u32_le().await? as usize;
				let mut link = vec![];
				link.resize(len, 0);
				read.read_exact(&mut link).await?;
				Some(link)
			} else {
				None
			};
			let idx = usize::MAX;
			let f = File {
				path: filename_buf.clone(),
				mtime: mtime_buf,
				symlink,
				mode,
				size,
				idx,
				client_id,
			};
			ret.push(f);
		}
		if read.read_i32_le().await? != 0 {
			log::warn!("IO errors while listing files.");
		}
		ret.sort_unstable_by(|a, b| a.path.cmp(&b.path));
		// Hmm. rsyn also dedupes. I've never seen dupes, so I don't know why.
		for (i, mut f) in ret.iter_mut().enumerate() {
			f.idx = i;
			log::debug!("{:>6} {}", f.idx, f);
		}
		Ok(ret)
	}
}

type Requests = Arc<SyncMutex<Option<HashMap<usize, Vec<mpsc::Sender<Result<Bytes, tokio::io::Error>>>>>>>;
type Enveloped = EnvelopeRead<BufReader<OwnedReadHalf>>;

struct ReadFilesProcess {
	read: Enveloped,
	reqs: Requests,
	current: Option<mpsc::Sender<Result<Bytes, tokio::io::Error>>>,
}

impl ReadFilesProcess {
	async fn run(mut self) -> Result<Enveloped> {
		let res = self.process().await;
		log::debug!("Client exiting: {:?}", res);
		use tokio::io::{Error, ErrorKind};
		let remaining = self.reqs.lock().unwrap().take();
		// TODO: When switching away from anyhow, make sure our error type is cloneable - and send
		// the real error.
		let cex = |msg: &str| Err(Error::new(ErrorKind::ConnectionAborted, anyhow::anyhow!("{}", msg)));
		if let Some(mut current) = self.current.take() {
			if res.is_err() {
				current.send(cex("Client exited while reading file")).await.ok();
			} else {
				current.send(cex("Internal panic!")).await.ok();
				unreachable!();
			}
		};
		for (_, cs) in remaining.unwrap().into_iter() {
			for mut c in cs.into_iter() {
				c.send(cex("Client exited")).await.ok();
			}
		}
		res?;
		Ok(self.read)
	}
	/// Phase 2 file receiver
	async fn process(&mut self) -> Result<()> {
		loop {
			let idx = self.read.read_i32_le().await?;
			// TODO: according to openrsync's rsync.5, requested indexes may be
			// 1. reordered
			// 2. silently skipped
			// So I may need some nifty logic to recognize that a file won't be sent...
			// (For now, library users will have to close() to get their files to "time out".)
			if idx == -1 {
				break Ok(());
			}
			log::debug!("Receiving file {}", idx);
			{
				// block info
				let mut buf = [0u8; 16];
				self.read.read_exact(&mut buf).await?;
				anyhow::ensure!(
					buf == [0u8; 16],
					"Protocol error: we requested a plain file, not blocks and checksums"
				);
			}
			let idx = idx as usize;
			self.current = {
				let mut table = self.reqs.lock().unwrap();
				let table = table.as_mut().unwrap();
				let mut reqs = table.get_mut(&idx);
				let req = reqs
					.as_mut()
					.and_then(|v| v.pop())
					.context("Got file - no memory of requesting it")?;
				if reqs.map(|v| v.len()) == Some(0) {
					table.remove(&idx).unwrap();
				}
				Some(req)
			};
			let mut size: usize = 0;
			loop {
				let chunklen = self.read.read_i32_le().await?;
				if chunklen == 0 {
					break;
				}
				anyhow::ensure!(chunklen > 0, "Protocol error: negative sized chunk");
				let mut chunklen = chunklen as usize;
				size += chunklen;
				while chunklen > 0 {
					let read = std::cmp::min(1 << 16, chunklen);
					let mut buf = BytesMut::new();
					buf.resize(read, 0);
					chunklen -= self.read.read_exact(&mut buf).await?;
					if let Some(backchan) = self.current.as_mut() {
						log::trace!("File {}: got part {}, {} remaining in chunk", idx, buf.len(), chunklen);
						if let Err(_e) = backchan.send(Ok(buf.into())).await {
							self.current = None;
							log::warn!("Internal close while receiving file: {} - ignoring", _e);
						}
					}
				}
			}
			{
				// Hash. TODO: check
				let mut buf = [0u8; 16];
				self.read.read_exact(&mut buf).await?;
				log::debug!(
					"Finished {} successfully, {} B, checksum {:?} not checked",
					idx,
					size,
					buf
				);
			}
			self.current = None; // Drop sender, finalizing internal transfer
		}
	}
}

#[derive(Clone, Debug)]
/// Information about a file on the rsync server
pub struct File {
	/// Path as returned by the server
	/// Take care to normalize it
	pub path: Vec<u8>,
	/// Symlink target, if [is_symlink](crate::File::is_symlink)
	pub symlink: Option<Vec<u8>>,
	mode: u32,
	/// File size in bytes
	pub size: u64,
	/// Modification time. Range is limited as it is parsed from an i32
	pub mtime: Option<DateTime<Utc>>,
	/// Position in file list
	idx: usize,
	/// Since a different client may have a different file list and files are requested by index,
	/// not by path, we must ensure that files and clients don't get mixed.
	client_id: usize,
}
impl File {
	/// e.g. the `unix_mode` crate is useful for parsing
	pub fn unix_mode(&self) -> u32 {
		self.mode
	}
	/// A regular file?
	pub fn is_file(&self) -> bool {
		unix_mode::is_file(self.mode)
	}
	/// A directory?
	pub fn is_directory(&self) -> bool {
		unix_mode::is_dir(self.mode)
	}
	/// A symlink? `symlink` will be `Some`.
	pub fn is_symlink(&self) -> bool {
		unix_mode::is_symlink(self.mode)
	}
}
impl std::fmt::Display for File {
	/// Print as if we're doing `ls -l`
	fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
		write!(
			fmt,
			"{} {:>12} {} {}{}",
			unix_mode::to_string(self.mode),
			self.size,
			self.mtime
				.as_ref()
				.map(DateTime::to_rfc3339)
				.unwrap_or("                    ".to_owned()),
			String::from_utf8_lossy(&self.path),
			self.symlink
				.as_ref()
				.map(|s| format!(" -> {}", String::from_utf8_lossy(&s)))
				.unwrap_or("".to_owned())
		)?;
		Ok(())
	}
}

/// Strips rsync data frame headers and prints non-data frames as warning messages
struct EnvelopeRead<T: tokio::io::AsyncBufRead + Unpin> {
	// TODO: rebuild with an enum of pending body, pending error, and pending header
	read: T,
	frame_remaining: usize,
	pending_error: Option<(u8, Vec<u8>)>,
	pending_header: Option<([u8; 4], u8)>,
}

impl<T: tokio::io::AsyncBufRead + Unpin> EnvelopeRead<T> {
	fn new(t: T) -> EnvelopeRead<T> {
		EnvelopeRead {
			read: t,
			frame_remaining: 0,
			pending_error: None,
			pending_header: None,
		}
	}

	fn poll_err(
		mut self: Pin<&mut Self>,
		ctx: &mut std::task::Context<'_>,
		repoll_buf: &mut [u8],
	) -> Poll<Result<usize, std::io::Error>> {
		while self.frame_remaining > 0 {
			let mut pe = self
				.pending_error
				.as_ref()
				.expect("Error expected, but not present")
				.1
				.clone();
			let pei = pe.len() - self.frame_remaining; // Ich check dich wek alter bowwochecka
			match Pin::new(&mut self.read).poll_read(ctx, &mut pe[pei..]) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Ready(Ok(0)) => break,
				Poll::Ready(Ok(r)) => {
					self.frame_remaining -= r;
					self.pending_error.as_mut().unwrap().1 = pe;
				}
			};
		}
		let (typ, msg) = self.pending_error.take().unwrap();
		let msg = String::from_utf8_lossy(&msg);
		let msg = msg
			.strip_suffix("\n")
			.filter(|msg| msg.matches("\n").count() == 0)
			.unwrap_or(&msg)
			.to_owned();
		let e = match typ {
			8 => {
				log::warn!("Sender: {}", msg);
				return self.poll_read(ctx, repoll_buf);
			}
			1 => anyhow::anyhow!("Server error: {}", msg),
			t => anyhow::anyhow!("Unknown error {}: {}", t, msg),
		};
		return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e)));
	}
}

impl<T: tokio::io::AsyncBufRead + Unpin> AsyncRead for EnvelopeRead<T> {
	fn poll_read(
		mut self: Pin<&mut Self>,
		ctx: &mut std::task::Context<'_>,
		buf: &mut [u8],
	) -> Poll<Result<usize, std::io::Error>> {
		if self.pending_error.is_some() {
			return self.poll_err(ctx, buf);
		}
		while self.frame_remaining == 0 {
			let no_pending_header = self.pending_header.is_none();
			let pll = Pin::new(&mut self.read).poll_fill_buf(ctx);
			match pll {
				// Starting to wonder whether it wouldn't be easier to store the future returend by read_u32_le
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
				Poll::Ready(Ok([])) if no_pending_header => return Poll::Ready(Ok(0)),
				Poll::Ready(Ok([])) => {
					return Poll::Ready(Err(std::io::Error::new(
						std::io::ErrorKind::ConnectionAborted,
						anyhow::anyhow!("Abort during header read"),
					)))
				}
				Poll::Ready(Ok(slice)) => {
					let slicehead = &mut [0u8; 4];
					let consumable = std::cmp::min(slice.len(), 4);
					slicehead[..consumable].copy_from_slice(&slice[..consumable]);
					let (mut ph, phl) = self.pending_header.take().unwrap_or(([0u8; 4], 0));
					let phl = phl as usize;
					let consumable = std::cmp::min(ph.len() - phl, consumable);
					ph[phl..(phl + consumable)].copy_from_slice(&slicehead[..consumable]);
					let phl = phl + consumable;
					Pin::new(&mut self.read).consume(consumable);
					if phl < ph.len() {
						self.pending_header = Some((ph, phl as u8));
					} else {
						let [b1, b2, b3, b4] = ph;
						let b1 = b1 as usize;
						let b2 = b2 as usize;
						let b3 = b3 as usize;
						self.frame_remaining = b1 + b2 * 0x100 + b3 * 0x100_00 as usize;
						log::trace!("Frame {} {}", b4, self.frame_remaining);
						match b4 {
							7 => (),
							t => {
								let mut errbuf = vec![];
								errbuf.resize(self.frame_remaining, 0);
								self.pending_error = Some((t, errbuf));
								return self.poll_err(ctx, buf);
							}
						};
					}
				}
			}
		}
		let request = std::cmp::min(buf.len(), self.frame_remaining as usize);
		Pin::new(&mut self.read).poll_read(ctx, &mut buf[0..request]).map(|r| {
			r.map(|r| {
				self.frame_remaining -= r;
				r
			})
		})
	}
}

/// Rsync's file list entry "header"
struct FileEntryStatus(u8);
impl FileEntryStatus {
	fn is_end(&self) -> bool {
		self.0 == 0
	}
	#[allow(dead_code)] // I have really no idea what it is good for
	fn is_top_level(&self) -> bool {
		self.0 & 0x01 != 0
	}
	fn file_mode_repeated(&self) -> bool {
		self.0 & 0x02 != 0
	}
	fn uid_repeated(&self) -> bool {
		self.0 & 0x08 != 0
	}
	fn gid_repeated(&self) -> bool {
		self.0 & 0x10 != 0
	}
	fn inherits_filename(&self) -> bool {
		self.0 & 0x20 != 0
	}
	fn integer_filename_len(&self) -> bool {
		self.0 & 0x40 != 0
	}
	fn mtime_repeated(&self) -> bool {
		self.0 & 0x80 != 0
	}
}

#[async_trait::async_trait]
trait RsyncReadExt: AsyncRead + Unpin {
	/// For reading rsync's variable length integers… quite an odd format.
	async fn read_rsync_long(&mut self) -> Result<i64> {
		let v = self.read_i32_le().await?;
		Ok(if v == -1 { self.read_i64_le().await? } else { v as i64 })
	}
}
impl<T: tokio::io::AsyncRead + Unpin> RsyncReadExt for T {}
