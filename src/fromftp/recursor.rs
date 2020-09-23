use crate::nabla::*;
use anyhow::{Context, Result};
use async_ftp::{FtpError, FtpStream};
use ignore::gitignore::Gitignore;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::result::Result as StdResult;

pub struct Recursor<'a> {
	ftp: &'a mut FtpStream,
	base: PathBuf,
	result: SyncInfo,
	ignore: &'a Gitignore,
}
impl<'a> Recursor<'a> {
	pub async fn run(ftp: &'a mut FtpStream, ignore: &'a Gitignore) -> Result<SyncInfo> {
		let wd = ftp
			.pwd()
			.await
			.context("Failed to retrieve absolute path for base directory")?;
		let mut r = Recursor {
			ftp,
			base: Path::new(&wd).to_path_buf(),
			result: SyncInfo::new(), // Marks the sync start time
			ignore,
		};
		r.rec().await?;
		Ok(r.result)
	}
	fn rec(&mut self) -> Pin<Box<dyn '_ + Send + Future<Output = Result<()>>>> {
		Box::pin(async move {
			let pth = &self.ftp.pwd().await.context("Cannot get current path")?;
			let pth = Path::new(pth).to_path_buf();
			let lst = self
				.ftp
				.nlst(None)
				.await
				.with_context(|| format!("Cannot list {:?}", pth))?;
			for ref f in lst {
				let fullname = &pth.clone().join(f);
				let name = pathdiff::diff_paths(fullname, self.base.clone()).with_context(|| {
					format!(
						"Internal: Can't get path {:?} relative to base {:?}",
						fullname, self.base
					)
				})?;
				let is_dir = self.ftp.cwd(f).await.is_ok();
				if self.ignore.matched(&name, is_dir).is_ignore() {
					log::debug!("Ignoring {:?}", name);
					if is_dir {
						self.ftp.cdup().await.context("Can't leave FTP directory.")?;
					}
					continue;
				}
				if is_dir {
					self.rec().await?;
					self.ftp.cdup().await.context("Can't leave FTP directory.")?;
				} else {
					// Appeared in the list but we can't CD to it? Assume it's a file
					let mt = self.ftp.mdtm(&f).await;
					let sz = self.ftp.size(&f).await;
					fn non_fatal_err<T>(e: &StdResult<T, FtpError>) -> bool {
						// TODO: Test? If we misinterpret a fatal as a non-fatal,
						// we may erroneously delete files from the top folder
						use FtpError::*;
						match e {
							Err(SecureError(_)) => true,
							Err(InvalidResponse(_)) => true,
							Err(ConnectionError(_)) => false,
							Err(InvalidAddress(_)) => false,
							Ok(_) => false,
						}
					};
					if non_fatal_err(&sz) && non_fatal_err(&mt) {
						log::debug!("MDTM {:?}: {:?}", name, mt);
						log::debug!("SIZE {:?}: {:?}", name, sz);
						log::warn!("Could not get mtime or size for {:?}, ignoring", name);
						continue;
					}
					self.result.files.insert(
						name,
						FileInfo {
							t: mt.with_context(|| format!("Can't get mtime for {:?}", fullname))?,
							s: sz.with_context(|| format!("Can't get size for {:?}", fullname))?,
							deleted: None,
						},
					);
				};
			}
			Ok(())
		})
	}
}

#[cfg(test)]
mod test {
	use super::super::memftp::*;
	use super::*;
	use std::collections::HashSet;

	#[tokio::test(threaded_scheduler)]
	pub async fn main() {
		let addr = unmemftp::serve(Box::new(abc)).await;

		let mut ftp_stream = FtpStream::connect(addr).await.unwrap();
		ftp_stream.login("anonymous", "onymous").await.unwrap();
		ftp_stream
			.transfer_type(async_ftp::types::FileType::Binary)
			.await
			.unwrap();
		let ups = Recursor::run(&mut memstream(Box::new(abc)).await, &Gitignore::empty())
			.await
			.unwrap();

		assert_eq!(
			ups.files
				.iter()
				.map(|(p, _)| p.to_path_buf())
				.collect::<HashSet<PathBuf>>(),
			vec![PathBuf::from("a"), PathBuf::from("b/c")]
				.into_iter()
				.collect::<HashSet<PathBuf>>(),
		);
	}
}
