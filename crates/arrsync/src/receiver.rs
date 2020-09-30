use crate::alluse::*;

pub struct ReadFilesProcess {
	read: Enveloped,
	reqs: Requests,
	current: Option<mpsc::Sender<Result<Bytes, tokio::io::Error>>>,
}

impl ReadFilesProcess {
	pub fn spawn(read: Enveloped, reqs: Requests) -> JoinHandle<Result<Enveloped>> {
		tokio::spawn(
			ReadFilesProcess {
				read,
				reqs,
				current: None,
			}
			.run(),
		)
	}
	pub async fn run(mut self) -> Result<Enveloped> {
		let res = self.process().await;
		log::debug!("Client exiting: {:?}", res);
		use tokio::io::{Error, ErrorKind};
		let remaining = self.reqs.lock().unwrap().requests.take();
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
			RequestsInner::refresh_timeout(&mut self.reqs);
			let idx = self.read.read_i32_le().await?;
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
				let mut requests = self.reqs.lock().unwrap();
				requests.disable_timeout();
				let table = requests.requests.as_mut().unwrap();
				let mut reqs = table.get_mut(&idx);
				let req = reqs
					.as_mut()
					.and_then(|v| v.pop())
					.context("Got file - no memory of requesting it (maybe timed out erroneously?)")?;
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

pub struct RequestsInner {
	pub requests: Option<HashMap<usize, Vec<mpsc::Sender<Result<Bytes, tokio::io::Error>>>>>,
	pub timeout: Option<Instant>,
}

impl RequestsInner {
	pub fn new_requests() -> Requests {
		Arc::new(SyncMutex::new(RequestsInner {
			requests: Some(default()),
			timeout: None,
		}))
	}

	pub fn refresh_timeout(reqs: &mut Requests) {
		// according to openrsync's rsync.5, requested indexes may be
		// 1. reordered
		// 2. silently skipped
		// So, if there are pending requests but no file has been requested or is being received for some time, we time
		// out all the pending requests.
		// If requests are made regularly based on some external events, this scheme will fall
		// flat. I could guard against that by limiting the number of pending requests, but that's
		// kind of against the rsync design.
		let mut reqs_inner = reqs.lock().unwrap();
		let spawn = reqs_inner.timeout.is_none();
		reqs_inner.timeout = Some(Instant::now() + Duration::from_secs_f64(30.)); // TODO: make configurable
		if spawn {
			tokio::spawn(Self::timeout_proc(reqs.clone()));
		}
	}

	pub fn disable_timeout(&mut self) {
		self.timeout.take();
	}

	async fn timeout_proc(reqs: Requests) {
		loop {
			enum Action {
				ReSleep(Instant),
				Timeout(Vec<mpsc::Sender<Result<Bytes, tokio::io::Error>>>),
			};
			use Action::*;
			let action = {
				let mut reqs = reqs.lock().unwrap();
				match reqs.timeout {
					None => break, // Timeout cancelled, currently transferring file
					Some(timeout) if timeout > Instant::now() => ReSleep(timeout),
					Some(_) => Timeout(
						reqs.requests
							.iter_mut()
							.flat_map(|m| m.drain())
							.flat_map(|(idx, rs)| {
								log::debug!("Timeouting {} requests for file {}", rs.len(), idx);
								rs.into_iter()
							})
							.collect(),
					),
				}
			};
			match action {
				ReSleep(until) => tokio::time::delay_until(until).await,
				Timeout(requests) => {
					for mut request in requests.into_iter() {
						tokio::spawn(async move {
							// Spawn, in case someone is waiting for the wrong thing to
							// timeout..
							// Probably unnecessary
							use std::io::{Error, ErrorKind};
							request
								.send(Err(Error::new(ErrorKind::TimedOut, anyhow::anyhow!("Timed out"))))
								.await
								.ok();
						});
					}
					break;
				}
			}
		}
	}
}
