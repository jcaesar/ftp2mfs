use tokio::net::TcpStream;
//type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use anyhow::{ Result, Context };
use tokio::io::AsyncWriteExt;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncBufReadExt;
use core::task::Poll;
use std::pin::Pin;
use tokio::io::{ AsyncBufRead, AsyncRead, BufReader };
use std::marker::Unpin;
use tokio::net::tcp::{ OwnedWriteHalf, OwnedReadHalf };
use chrono::prelude::*;
use bytes::{ Bytes, BytesMut };
fn default<T: Default>() -> T { std::default::Default::default() } // default_free_fn
use tokio::sync::mpsc;
use std::sync::{ Arc, Mutex as SyncMutex };
use std::collections::HashMap;
use tokio::sync::Mutex as AsyncMutex;
use std::sync::atomic::{ AtomicUsize, Ordering as AtomicOrder };

static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct RsyncClient {
    inner: Arc<RsyncClientInner>
}

struct RsyncClientInner {
    reqs: Requests,
    write: AsyncMutex<OwnedWriteHalf>,
    finish: SyncMutex<Option<tokio::task::JoinHandle<Result<Enveloped>>>>,
    id: usize,
}

/// read/written as viewed by server(?)
#[derive(Clone, Debug)]
pub struct Stats {
    bytes_read: i64,
    bytes_written: i64,
    file_size: i64
}

impl RsyncClient {
    pub async fn connect(url: url::Url) -> Result<(RsyncClient, Vec<File>)> {
        let (path, base) = Self::parse_url(&url)?;
        let (read, mut write) = Self::stream(&url).await?;
        let mut read = BufReader::new(read);
        Self::send_handshake(&mut write, path, base).await?;
        Self::read_handshake(&mut read, &url.origin().unicode_serialization()).await?;
        let mut read = EnvelopeRead::new(read); // Server multiplex start
        let id = NEXT_CLIENT_ID.fetch_add(1, AtomicOrder::SeqCst);
        let files = Self::read_file_list(&mut read, id).await?;
        write.write_i32_le(-1).await?; anyhow::ensure!(read.read_i32_le().await? == -1, "Phase switch receive-list -> receive-file");
        let reqs: Requests = Arc::new(SyncMutex::new(Some(default())));
        let process = tokio::spawn(ReadFilesProcess {
            read,
            reqs: reqs.clone(),
            current: None,
        }.run());
        let inner = RsyncClientInner {
            reqs: reqs.clone(),
            write: AsyncMutex::new(write),
            finish: SyncMutex::new(Some(process)),
            id,
        };
        Ok((RsyncClient { inner: Arc::new(inner) }, files))
    }
    pub async fn get(&self, file: &File) -> Result<impl AsyncRead> {
        anyhow::ensure!(self.inner.id == file.client_id, "Requested file from client that was not listed in that client's connect call");
        anyhow::ensure!(file.is_file(), "Can only request files, {} is not", file);
        let (data_send, data_recv) = mpsc::channel(10);
        self.inner.reqs.lock().unwrap()
            .as_mut()
            .context("Client fully exited")?
            .entry(file.idx)
            .or_insert(vec![])
            .push(data_send);
        let mut write = self.inner.write.lock().await;
        write.write_i32_le(file.idx as i32).await?;
        write.write_all(&[0u8; 16]).await?; // We want no blocks, blocklength, checksum, or terminal block. 4 x 0i32 = 16
        Ok(tokio::io::stream_reader(data_recv))
    }
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
           path.strip_prefix(Path::new("/")).expect(&format!("Stripping root from path {:?}", path))
           // Does this work on windows?
        } else { path };
        let mut base = path;
        let base = loop {
            match base.parent() {
                Some(p) if p == Path::new("") => break base,
                None => panic!("Getting base element of path {:?}", ),
                Some(p) => base = p,
            };
        };
        let base = base.to_str().expect("TODO: handle paths properly");
        Ok((path, base))
    }
    async fn stream(url: &url::Url) -> Result<(OwnedReadHalf, OwnedWriteHalf)> {
        let mut addrs = url.socket_addrs(|| Some(873))
            .context(format!("Get socket addr from url {}", url))?;
        let mut stream = TcpStream::connect(
            addrs.pop().expect("Successful resolution returns at least one address")
        ).await;
        for addr in addrs.iter() {
            stream = TcpStream::connect(addr).await;
            if stream.is_ok() {
                break;
            }
        }
        Ok(stream.context(format!("Connect to {} ({:?})", url, addrs))?.into_split())
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
        ].concat();
        write.write_all(&initial).await?;
        Ok(())
    }
    async fn read_handshake<T: AsyncBufRead + Unpin>(read: &mut T, origin: &str) -> Result<()> {
        let hello = &mut String::new();
        read.read_line(hello)
            .await.context("Read server hello")?;
        let rsyncd = "@RSYNCD: ";
        anyhow::ensure!(hello.starts_with(rsyncd), "Not an rsync server? First message was {}", hello);
        let ver = hello[rsyncd.len() .. (hello.len() - 1)]
            .split('.').map(str::parse)
            .collect::<Result<Vec<u64>, _>>()
            .context(format!("Parsing server version from {}", hello))?;
        anyhow::ensure!(ver >= vec![27, 0], "Server version {} not supported - need 27.0 minimum.",
            ver.iter().map(|i| format!("{}", i)).collect::<Vec<_>>().join(".")
        );
        let mut motd = String::new();
        loop {
            let select = &mut String::new();
            read.read_line(select)
                .await.context("Read server startup")?;
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
            let inherited_filename_length = if meta.inherits_filename() { read.read_u8().await? as usize } else { 0 };
            anyhow::ensure!(inherited_filename_length <= filename_buf.len(), "Protocol error: file list format inconsistency");
            let filename_length = if meta.integer_filename_len() {
                read.read_u32_le().await? as usize 
            } else {
                read.read_u8().await? as usize
            };
            filename_buf.resize(filename_length + inherited_filename_length, 0);
            read.read_exact(&mut filename_buf[inherited_filename_length .. ]).await?;
            let show = String::from_utf8_lossy(&filename_buf);
            let size = read.read_rsync_long().await?;
            anyhow::ensure!(size >= 0, "Protocol error: negative file size for {}", show);
            let size = size as u64;
            mtime_buf = if meta.mtime_repeated() { mtime_buf } else {
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
            } else { None };
            let idx = usize::MAX;
            let f = File {
                path: filename_buf.clone(),
                mtime: mtime_buf,
                symlink, mode, size, idx, client_id,
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
        use tokio::io::{ Error, ErrorKind };
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
            { // block info
                let mut buf = [0u8; 16];
                self.read.read_exact(&mut buf).await?;
                anyhow::ensure!(buf == [0u8; 16], "Protocol error: we requested a plain file, not blocks and checksums");
            }
            let idx = idx as usize;
            self.current = {
                let mut table = self.reqs.lock().unwrap();
                let table = table.as_mut().unwrap();
                let mut reqs = table.get_mut(&idx);
                let req = reqs.as_mut().and_then(|v| v.pop()).context("Got file - no memory of requesting it")?;
                if reqs.map(|v| v.len()) == Some(0) { table.remove(&idx).unwrap(); }
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
                    let mut buf = BytesMut::with_capacity(read);
                    chunklen -= self.read.read_buf(&mut buf).await?;
                    if let Some(backchan) = self.current.as_mut() {
                        log::trace!("File {}: got part {}, {} remaining in chunk", idx, buf.len(), chunklen);
                        if let Err(_e) = backchan.send(Ok(buf.into())).await {
                            self.current = None;
                            log::warn!("Internal close while receiving file: {} - ignoring", _e);
                        }
                    }
                }
            }
            { // Hash. TODO: check
                let mut buf = [0u8; 16];
                self.read.read_exact(&mut buf).await?;
                log::debug!("Finished {} successfully, {} B, checksum {:?} not checked", idx, size, buf);
            }
            self.current = None; // Drop sender, finalizing internal transfer
        }
    }
}

#[derive(Clone, Debug)]
pub struct File {
    pub path: Vec<u8>,
    pub symlink: Option<Vec<u8>>,
    mode: u32,
    pub size: u64,
    pub mtime: Option<DateTime<Utc>>,
    idx: usize,
    client_id: usize,
}
impl File {
    /// e.g. the unix_mode crate is useful for parsing
    pub fn unix_mode(&self) -> u32 { self.mode }
    pub fn is_file(&self) -> bool { unix_mode::is_file(self.mode) }
    pub fn is_directory(&self) -> bool { unix_mode::is_dir(self.mode) }
    pub fn is_symlink(&self) -> bool { unix_mode::is_symlink(self.mode) }
}
impl std::fmt::Display for File {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "{} {:>12} {} {}{}",
            unix_mode::to_string(self.mode),
            self.size,
            self.mtime.as_ref().map(DateTime::to_rfc3339).unwrap_or("                    ".to_owned()),
            String::from_utf8_lossy(&self.path),
            self.symlink.as_ref().map(|s| format!(" -> {}", String::from_utf8_lossy(&s))).unwrap_or("".to_owned())
        )?;
        Ok(())
    }
}

struct EnvelopeRead<T: tokio::io::AsyncBufRead + Unpin> {
    read: T,
    frame_remaining: usize,
    pending_error: Option<(u8, Vec<u8>)>,
}

impl<T: tokio::io::AsyncBufRead + Unpin> EnvelopeRead<T> {
    fn new(t: T) -> EnvelopeRead<T> { EnvelopeRead { read: t, frame_remaining: 0, pending_error: None } }

    fn poll_err(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>, repoll_buf: &mut [u8])
        -> Poll<Result<usize, std::io::Error>>
    {
        while self.frame_remaining > 0 {
            let mut pe = self.pending_error.as_ref().expect("Error expected, but not present").1.clone();
            let pei = pe.len() - self.frame_remaining; // Ich check dich wek alter bowwochecka
            match Pin::new(&mut self.read).poll_read(
                ctx, &mut pe[pei..]
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(r)) => {
                    self.frame_remaining -= r;
                    self.pending_error.as_mut().unwrap().1 = pe;
                },
            };
        }
        let (typ, msg) = self.pending_error.take().unwrap();
        let msg = String::from_utf8_lossy(&msg);
        let msg = msg.strip_suffix("\n").filter(|msg| msg.matches("\n").count() == 0).unwrap_or(&msg).to_owned();
        let e = match typ {
            8 => {
                log::warn!("Sender: {}", msg);
                return self.poll_read(ctx, repoll_buf);
            },
            1 => anyhow::anyhow!("Server error: {}", msg),
            t => anyhow::anyhow!("Unknown error {}: {}", t, msg),
        };
        return Poll::Ready(Err(
            std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e)
        ));
    }
}

impl<T: tokio::io::AsyncBufRead + Unpin> AsyncRead for EnvelopeRead<T> {
    fn poll_read(mut self: Pin<&mut Self>, ctx: &mut std::task::Context<'_>, buf: &mut [u8])
        -> Poll<Result<usize, std::io::Error>>
    {
        if self.pending_error.is_some() {
            return self.poll_err(ctx, buf);
        }
        while self.frame_remaining == 0 {
            let pll = Pin::new(&mut self.read).poll_fill_buf(ctx);
            match pll {
                // Starting to wonder whether it wouldn't be easier to store the future returend by read_u32_le
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok([])) => return Poll::Ready(Ok(0)),
                Poll::Ready(Ok([b1, b2, b3, b4, ..])) => {
                    let b1 = *b1 as usize; let b2 = *b2 as usize; let b3 = *b3 as usize; let b4 = *b4;
                    Pin::new(&mut self.read).consume(4);
                    self.frame_remaining = b1 + b2 * 0x100 + b3 * 0x100_00;
                    log::trace!("Frame {}", self.frame_remaining);
                    match b4 {
                        7 => (),
                        t => {
                            let mut errbuf = vec![];
                            errbuf.resize(self.frame_remaining, 0);
                            self.pending_error = Some((t, errbuf));
                            return self.poll_err(ctx, buf);
                        }
                    };
                    break;
                },
                Poll::Ready(Ok(_)) => continue,
            }
        }
        let request = std::cmp::min(buf.len(), self.frame_remaining as usize);
        Pin::new(&mut self.read).poll_read(ctx, &mut buf[0..request])
            .map(|r| r.map(|r| { self.frame_remaining -= r; r }))
    }
}

struct FileEntryStatus(u8);
impl FileEntryStatus {
    fn is_end(&self) -> bool { self.0 == 0 }
    #[allow(dead_code)] // I have really no idea what it is good for
    fn is_top_level(&self) -> bool { self.0 & 0x01 != 0 }
    fn file_mode_repeated(&self) -> bool { self.0 & 0x02 != 0 }
    fn uid_repeated(&self) -> bool { self.0 & 0x08 != 0 }
    fn gid_repeated(&self) -> bool { self.0 & 0x10 != 0 }
    fn inherits_filename(&self) -> bool { self.0 & 0x20 != 0 }
    fn integer_filename_len(&self) -> bool { self.0 & 0x40 != 0 }
    fn mtime_repeated(&self) -> bool { self.0 & 0x80 != 0 }
}

#[async_trait::async_trait] 
trait RsyncReadExt: AsyncRead + Unpin {
    async fn read_rsync_long(&mut self) -> Result<i64> {
        let v = self.read_i32_le().await?;
        Ok(if v == -1 {
            self.read_i64_le().await?
        } else {
            v as i64
        })
    }
}
impl<T: tokio::io::AsyncRead + Unpin> RsyncReadExt for T {}
