use crate::nabla::*;
use ftp::{ FtpStream, FtpError };
use std::path::{ Path, PathBuf };
use anyhow::{ Result, Context };
use std::result::Result as StdResult;
use ignore::gitignore::Gitignore;

pub struct Recursor<'a> {
	ftp: &'a mut FtpStream,
	base: PathBuf,
	result: SyncInfo,
	ignore: &'a Gitignore,
}
impl <'a> Recursor <'a> {
	pub fn run(ftp: &'a mut FtpStream, ignore: &'a Gitignore) -> Result<SyncInfo> {
		let wd = ftp.pwd()
			.context("Failed to retrieve absolute path for base directory")?;
		let mut r = Recursor {
			ftp,
			base: Path::new(&wd).to_path_buf(),
			result: SyncInfo::new(), // Marks the sync start time
			ignore,
		};
		r.rec()?;
		Ok(r.result)
	}
	fn rec(&mut self) -> Result<()> {
		let pth = &self.ftp.pwd()
			.context("Cannot get current path")?;
		let pth = Path::new(pth).to_path_buf();
		let lst = self.ftp.nlst(None)
			.with_context(|| format!(""))?;
		for ref f in lst {
			let fullname = &pth.clone().join(f);
			let name = pathdiff::diff_paths(fullname, self.base.clone())
				.with_context(|| format!("Internal: Can't get path {:?} relative to base {:?}", fullname, self.base))?;
			let is_dir = self.ftp.cwd(f).is_ok();
			if self.ignore.matched(&name, is_dir).is_ignore() {
				if is_dir {
					self.ftp.cdup()
						.context("Can't leave FTP directory.")?;
				}
				continue;
			}
			if is_dir {
				self.rec()?;
				self.ftp.cdup()
					.context("Can't leave FTP directory.")?;
			} else {
				// Appeared in the list but we can't CD to it? Assume it's a file
				let mt = self.ftp.mdtm(&f);
				let sz = self.ftp.size(&f);
				fn non_fatal_err<T>(e: &StdResult<T, FtpError>) -> bool {
					// TODO: Test? If we misinterpret a fatal as a non-fatal,
					// we may erroneously delete files from the top folder
					use FtpError::*;
					match e {
						Err(SecureError(_)) => true, Err(InvalidResponse(_)) => true,
						Err(ConnectionError(_)) => false, Err(InvalidAddress(_)) => false,
						Ok(_) => false,
					}
				};
				if non_fatal_err(&sz) && non_fatal_err(&mt) {
					// Neither size nor time are available -- assume access denied
					// TODO: log warning
					continue;
				}
				self.result.files.insert(name, FileInfo {
					t: mt.with_context(|| format!("Cant get mtime for {:?}", fullname))?,
					s: sz.with_context(|| format!("Cant get size for {:?}", fullname))?,
					deleted: None,
				});
			};
		};
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use super::super::memftp::*;
	use std::collections::HashSet;

	#[tokio::test(threaded_scheduler)]
	pub async fn main() {
		let addr = unmemftp::serve(Box::new(abc)).await;

		let mut ftp_stream = FtpStream::connect(addr).unwrap();
		ftp_stream.login("anonymous", "onymous").unwrap();
		ftp_stream.transfer_type(ftp::types::FileType::Binary).unwrap();
		let ups = Recursor::run(&mut memstream(Box::new(abc)).await, &Gitignore::empty()).unwrap();

		assert_eq!(
			ups.files.iter().map(|(p,_)| p.to_path_buf()).collect::<HashSet<PathBuf>>(),
			vec![PathBuf::from("a"), PathBuf::from("b/c")].into_iter().collect::<HashSet<PathBuf>>(),
		);
	}
}
