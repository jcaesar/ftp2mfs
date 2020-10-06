use crate::alluse::*;

static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
/// The main client struct
pub struct RsyncClient {
	inner: Arc<RsyncClientInner>,
}

struct RsyncClientInner {
	reqs: Requests,
	write: AsyncMutex<OwnedWriteHalf>,
	finish: SyncMutex<Option<JoinHandle<Result<Enveloped>>>>,
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
		let origin = format!(
			"{}{}",
			url.host().expect("Connected to an url without host"),
			url.port().map(|p| format!("{}", p)).unwrap_or("".to_string())
		);
		Self::read_handshake(&mut read, &origin).await?;
		let mut read = EnvelopeRead::new(read); // Server multiplex start
		let id = NEXT_CLIENT_ID.fetch_add(1, AtomicOrder::SeqCst);
		let files = Self::read_file_list(&mut read, id).await?;
		write.write_i32_le(-1).await?;
		anyhow::ensure!(
			read.read_i32_le().await? == -1,
			"Phase switch receive-list -> receive-file"
		);
		let reqs: Requests = RequestsInner::new_requests();
		let process = ReadFilesProcess::spawn(read, reqs.clone());
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
		{
			let mut reqs = self.inner.reqs.lock().unwrap();
			reqs.refresh_timeout();
			reqs.requests
				.as_mut()
				.context("Client fully exited")?
				.entry(file.idx)
				.or_insert(vec![])
				.push(data_send);
		}
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
