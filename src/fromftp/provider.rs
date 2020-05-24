use crossbeam_channel::{ unbounded, bounded };
use crossbeam_channel::{ Sender, Receiver };
use std::io::Cursor;
use std::thread;
use std::io::prelude::*;
use std::path::{ PathBuf, Path };
use ftp::FtpStream;

pub struct FtpProvider {
	mkreq: Sender<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>,
}
impl FtpProvider {
	pub fn new(mut ftp: FtpStream) -> FtpProvider {
		let (sender, receiver) = unbounded::<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>();
		thread::spawn(move || {
			while let Ok((path, chan)) = receiver.recv() {
				let retr = ftp.retr(path.to_str().unwrap(), |r| {
					let mut bufsize = 1<<16;
					loop {
						let mut buf = Vec::new();
						buf.resize(bufsize, 0);
						match r.read(&mut buf) {
							Ok(n) => {
								buf.truncate(n);
								if !chan.send(Ok(buf)).is_ok() {
									break;
								}
								if n == 0 {
									break;
								}
								bufsize = std::cmp::max(n * 9 / 8, 4096);
							},
							Err(e) => match e.kind() {
									std::io::ErrorKind::Interrupted => continue,
									_ => { chan.send(Err(e)).ok(); break }
							}
						}

					}
					Ok(())
				});
				if let Err(e) = retr {
					chan.send(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))).ok();
				}
			}
			ftp.quit().unwrap();
		});
		FtpProvider { mkreq: sender } 
	}
}

impl crate::provider::Provider for FtpProvider {
	fn get(&self, p: &Path) -> Box<dyn Read + Send + Sync> {
		let (sender, receiver) = bounded(128);
		self.mkreq.send((p.to_path_buf(), sender)).unwrap();
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
			let nextbuf = self.receiver.recv()
				.map_err(|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))??;
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
