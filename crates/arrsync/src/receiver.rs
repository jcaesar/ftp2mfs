use crate::alluse::*;

pub struct ReadFilesProcess {
	pub read: Enveloped,
	pub reqs: Requests,
	pub current: Option<mpsc::Sender<Result<Bytes, tokio::io::Error>>>,
}

impl ReadFilesProcess {
	pub async fn run(mut self) -> Result<Enveloped> {
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
