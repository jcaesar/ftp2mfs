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
use futures::executor::block_on;
use anyhow::Context;

pub struct FtpProvider {
	mkreq: UnboundedSender<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>,
}
impl FtpProvider {
    // All of this may be neater with coprocs/generators. Some day
	pub fn new(mut ftp: FtpStream) -> FtpProvider {
		let (sender, mut receiver) = unbounded::<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>();
		tokio::spawn(async move {
			while let Some((path, mut chan)) = receiver.next().await {
				let retr = ftp.retr(path.to_str().unwrap(), |mut r: BufReader<DataStream>| {
                    let mut chan = chan.clone();
                    async move {
                        let mut bufsize = 1<<16;
                        loop {
                            let mut buf = Vec::new();
                            buf.resize(bufsize, 0);
                            match tokio::io::AsyncReadExt::read(&mut r, &mut buf).await {
                                Ok(n) => {
                                    buf.truncate(n);
                                    if !chan.send(Ok(buf)).await.is_ok() {
                                        break;
                                    }
                                    if n == 0 {
                                        break;
                                    }
                                    bufsize = std::cmp::max(n * 9 / 8, 4096);
                                },
                                Err(e) => match e.kind() {
                                        std::io::ErrorKind::Interrupted => continue,
                                        _ => { chan.send(Err(e)).await.ok(); break }
                                }
                            }

                        }
                        Ok(())
                    }
                }).await;
				if let Err(e) = retr {
					chan.send(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))).await.ok();
				}
			}
			ftp.quit().await.ok();
		});
		FtpProvider { mkreq: sender } 
	}
}

#[async_trait::async_trait]
impl crate::provider::Provider for FtpProvider {
	fn get(&self, p: &Path) -> Box<dyn Read + Send + Sync> {
		let (sender, receiver) = bounded(128);
        println!("prepget: {:?}", p);
		self.mkreq.unbounded_send((p.to_path_buf(), sender)).expect("FTP client unexpectedly exited");
		Box::new(ChannelReader { receiver, current: Cursor::new(vec![]) })
	}
}

// IterRead won't do, we need to own that channel
struct ChannelReader {
	receiver: Receiver<Result<Vec<u8>, std::io::Error>>,
	current: Cursor<Vec<u8>>,
}
impl Read for ChannelReader {
	fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
		let read = self.current.read(buf)?;
		if read == 0 {
			let nextbuf = block_on(self.receiver.next()) // TODO: sucks, but we're not an AsyncRead
                .context("FTP EoF").map_err(|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))??;
            println!("Chan recv {}", nextbuf.len());
			if nextbuf.len() == 0 {
				return Ok(0);
			}
			std::mem::swap(&mut self.current, &mut Cursor::new(nextbuf));
			return self.current.read(buf);
		} else {
			Ok(read)
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use super::super::memftp::*;
	use crate::provider::Provider;

	#[tokio::test(threaded_scheduler)]
	pub async fn get_a() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await);
		assert_eq!(
			prov.get(Path::new("a")).bytes().collect::<Result<Vec<u8>, _>>().unwrap(),
			"Test file 1".as_bytes(),
		);
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn get_bc() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await);
		assert_eq!(
			prov.get(Path::new("b/c")).bytes().collect::<Result<Vec<u8>, _>>().unwrap(),
			"Test file in a subdirectory".as_bytes(),
		);
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn get_bc_absdir() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await);
		assert_eq!(
			prov.get(Path::new("/b/c")).bytes().collect::<Result<Vec<u8>, _>>().unwrap(),
			"Test file in a subdirectory".as_bytes(),
		);
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn cant_get_d() {
		let prov = FtpProvider::new(memstream(Box::new(abc)).await);
		assert!(
			prov.get(Path::new("/d")).bytes().collect::<Result<Vec<u8>, _>>().is_err()
		);
	}
}
