use std::io::Cursor;
use std::io::prelude::*;
use std::path::{ PathBuf, Path };
use async_ftp::{ FtpStream, DataStream };
use tokio::io::BufReader;
use futures_channel::mpsc::{ Sender, Receiver };
use futures_channel::mpsc::channel as bounded;
use futures_channel::mpsc::{ unbounded, UnboundedSender };
use futures::stream::StreamExt;
use futures_util::sink::SinkExt;
use futures::AsyncRead;
use anyhow::Context as _;
use url::Url;
use std::task::{ Poll, Context };
use std::pin::Pin;

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
                let retr = ftp.retr(path.to_str().unwrap(), |mut r: BufReader<DataStream>| {
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
                                    if let Err(e) = chan.send(Ok(buf)).await {
                                        log::warn!("Receiving: write pipe broke: {:?}", e);
                                        break;
                                    }
                                    if n == 0 {
                                        log::trace!("Receiving: finished with 0 block");
                                        break;
                                    }
                                    bufsize = std::cmp::min(std::cmp::max(1 << 16, n * 9 / 8), 1 << 23);
                                },
                                Err(e) =>{
                                    log::debug!("Receiving: Error {:?}", e);
                                    match e.kind() {
                                        std::io::ErrorKind::Interrupted => continue,
                                        _ => { return Err(async_ftp::FtpError::ConnectionError(e)); }
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                }).await;
				if let Err(e) = retr {
                    log::warn!("Retrieving {:?} failed: {:?}", path, e);
					chan.send(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))).await.ok();
				} else {
                    log::debug!("Retrieving {:?} succeeded", path);
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
		self.mkreq.unbounded_send((p.to_path_buf(), sender)).expect("FTP client unexpectedly exited");
		Ok(Box::new(ChannelReader { receiver, current: Cursor::new(vec![]) }))
	}
	fn base(&self) -> &Url { &self.base }
}

// IterRead won't do, we need to own that channel
struct ChannelReader {
	receiver: Receiver<Result<Vec<u8>, std::io::Error>>,
	current: Cursor<Vec<u8>>,
}
impl AsyncRead for ChannelReader {
	fn poll_read(self: Pin<&mut Self>, ctx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, std::io::Error>> {
		let selb = self.get_mut();
		let read = selb.current.read(buf)?;
		if read == 0 {
			match futures_core::stream::Stream::poll_next(Pin::new(&mut selb.receiver), ctx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Some(Ok(v))) if v.len() == 0 => {
                    log::trace!("Finished read");
                    return Poll::Ready(Ok(0));
                },
				Poll::Ready(next) => {
					let next = next.context("void EoF").map_err(
						|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e)
					)??;
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
	use super::*;
	use super::super::memftp::*;
	use crate::provider::Provider;
	use futures::io::AsyncReadExt;

	fn url() -> Url { Url::parse("ftp://example.example/example").unwrap() }

	#[tokio::test(threaded_scheduler)]
	pub async fn get_a() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url()).unwrap();
		let content = &mut String::new();
		prov.get(Path::new("a")).read_to_string(content).await.unwrap();
		assert_eq!(content, "Test file 1");
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn get_bc() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url()).unwrap();
		let content = &mut String::new();
		prov.get(Path::new("b/c")).read_to_string(content).await.unwrap();
		assert_eq!(content, "Test file in a subdirectory");
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn get_bc_absdir() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url()).unwrap();
		let content = &mut String::new();
		prov.get(Path::new("/b/c")).read_to_string(content).await.unwrap();
		assert_eq!(content, "Test file in a subdirectory");
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn cant_get_d() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await, url()).unwrap();
		let dummy = &mut String::new();
		assert!(prov.get(Path::new("/d")).read_to_string(dummy).await.is_err());
	}
}
