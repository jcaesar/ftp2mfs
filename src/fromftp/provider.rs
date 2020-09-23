use anyhow::Context as _;
use async_ftp::{DataStream, FtpStream};
use futures::stream::StreamExt;
use futures::AsyncRead;
use futures_channel::mpsc::channel as bounded;
use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_channel::mpsc::{Receiver, Sender};
use futures_util::sink::SinkExt;
use std::io::prelude::*;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::BufReader;
use url::Url;

#[derive(Clone)]
pub struct FtpProvider {
	mkreq: UnboundedSender<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>,
	base: Url,
}
impl FtpProvider {
	// All of this may be neater with coprocs/generators. Some day
	pub fn new(mut ftp: FtpStream, base: Url) -> FtpProvider {
		let (sender, mut receiver) = unbounded::<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>();
		log::debug!("Starting FTP file provider");
		tokio::spawn(async move {
			log::debug!("FTP file provider started");
			while let Some((path, mut chan)) = receiver.next().await {
				log::debug!("FTP file provider reqesting {:?}", path);
				let retr = ftp
					.retr(path.to_str().unwrap(), |mut r: BufReader<DataStream>| {
						let mut chan = chan.clone();
						async move {
							log::debug!("FTP file provider receiving");
							let mut bufsize = 1 << 16;
							loop {
								let mut buf = Vec::new();
								buf.resize(bufsize, 0);
								match tokio::io::AsyncReadExt::read(&mut r, &mut buf).await {
									Ok(n) => {
										log::trace!("Receiving: {} / {}", n, bufsize);
										buf.truncate(n);
										if n == 0 {
											log::trace!("Receiving: finished with 0 block");
											break;
										}
										if let Err(e) = chan.send(Ok(buf)).await {
											log::warn!("Receiving: write pipe broke: {:?}", e);
											break;
										}
										bufsize = std::cmp::min(std::cmp::max(1 << 16, n * 9 / 8), 1 << 23);
									}
									Err(e) => {
										log::debug!("Receiving: Error {:?}", e);
										match e.kind() {
											std::io::ErrorKind::Interrupted => continue,
											_ => {
												return Err(async_ftp::FtpError::ConnectionError(e));
											}
										}
									}
								}
							}
							Ok(())
						}
					})
					.await;
				if let Err(e) = retr {
					log::warn!("Retrieving {:?} failed: {:?}", path, e);
					chan.send(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e)))
						.await
						.ok();
				} else {
					log::debug!("Retrieving {:?} succeeded", path);
					chan.send(Ok(vec![])).await.ok();
				}
			}
			ftp.quit().await.ok();
		});
		FtpProvider { mkreq: sender, base }
	}
}

#[async_trait::async_trait]
impl crate::suite::Provider for FtpProvider {
	async fn get(&self, p: &Path) -> anyhow::Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
		let (sender, receiver) = bounded(128);
		self.mkreq
			.unbounded_send((p.to_path_buf(), sender))
			.expect("FTP client unexpectedly exited");
		Ok(Box::new(ChannelReader {
			receiver,
			current: Cursor::new(vec![]),
		}))
	}
	fn base(&self) -> &Url {
		&self.base
	}
}

// IterRead won't do, we need to own that channel
struct ChannelReader {
	receiver: Receiver<Result<Vec<u8>, std::io::Error>>,
	current: Cursor<Vec<u8>>,
}
impl AsyncRead for ChannelReader {
	fn poll_read(
		self: Pin<&mut Self>,
		ctx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<Result<usize, std::io::Error>> {
		let selb = self.get_mut();
		let read = selb.current.read(buf)?;
		if read == 0 {
			match futures_core::stream::Stream::poll_next(Pin::new(&mut selb.receiver), ctx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Some(Ok(v))) if v.len() == 0 => {
					log::trace!("Finished read");
					return Poll::Ready(Ok(0));
				}
				Poll::Ready(next) => {
					let next = next
						.context("void EoF")
						.map_err(|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))??;
					std::mem::swap(&mut selb.current, &mut Cursor::new(next));
				}
			};
			return Poll::Ready(selb.current.read(buf));
		} else {
			Poll::Ready(Ok(read))
		}
	}
}

#[cfg(test)]
mod test {
	use super::super::memftp::*;
	use super::*;
	use crate::suite::Provider;
	use futures::io::AsyncReadExt;

	#[allow(dead_code)]
	fn yeslog() {
		env_logger::builder()
			.filter_level(log::LevelFilter::Trace)
			.is_test(true)
			.try_init()
			.ok();
	}
	// Problem is that logs from threads won't be properly suppressed for passing threads
	// so enable it only when debugging a failing test

	fn url() -> Url {
		Url::parse("ftp://example.example/example").unwrap()
	}

	#[tokio::test(basic_scheduler)]
	pub async fn get_a() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url());
		let content = &mut String::new();
		prov.get(Path::new("a"))
			.await
			.unwrap()
			.read_to_string(content)
			.await
			.unwrap();
		assert_eq!(content, "Test file 1");
	}

	#[tokio::test(basic_scheduler)]
	pub async fn get_bc() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url());
		let content = &mut String::new();
		prov.get(Path::new("b/c"))
			.await
			.unwrap()
			.read_to_string(content)
			.await
			.unwrap();
		assert_eq!(content, "Test file in a subdirectory");
	}

	#[tokio::test(basic_scheduler)]
	pub async fn get_bc_absdir() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url());
		let content = &mut String::new();
		prov.get(Path::new("/b/c"))
			.await
			.unwrap()
			.read_to_string(content)
			.await
			.unwrap();
		assert_eq!(content, "Test file in a subdirectory");
	}

	#[tokio::test(basic_scheduler)]
	pub async fn cant_get_d() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url());
		let dummy = &mut String::new();
		let res = prov.get(Path::new("/d")).await.unwrap().read_to_string(dummy).await;
		assert!(res.is_err());
	}
}
