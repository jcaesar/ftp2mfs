use crate::nabla::*;
use ftp::FtpStream;
use std::path::{ Path, PathBuf };
use anyhow::{ Result, Context };
use globset::GlobSet;

pub struct Recursor<'a> {
	ftp: &'a mut FtpStream,
	base: PathBuf,
	result: SyncInfo,
	ignore: &'a GlobSet,
}
impl <'a> Recursor <'a> {
	pub fn run(ftp: &'a mut FtpStream, ignore: &'a GlobSet) -> Result<SyncInfo> {
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
			if self.ignore.is_match(&name) {
				continue;
			}
			match self.ftp.cwd(f) {
				Ok(()) => {
					self.rec()?;
					self.ftp.cdup()
						.context("Can't leave FTP directory. WTF?")?;
				},
				Err(_) => {
					// Appeared in the list but we can't CD to it? Assume it's a file
					// TODO: permission denied errors
					self.result.files.insert(name, FileInfo {
						t: self.ftp.mdtm(&f).with_context(|| format!("Cant get mtime for {:?}", fullname))?,
						s: self.ftp.size(&f).with_context(|| format!("Cant get size for {:?}", fullname))?,
						deleted: None,
					});
				},
			}
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
		let ups = Recursor::run(&mut memstream(Box::new(abc)).await, &GlobSet::empty()).unwrap();

		assert_eq!(
			ups.files.iter().map(|(p,_)| p.to_path_buf()).collect::<HashSet<PathBuf>>(),
			vec![PathBuf::from("a"), PathBuf::from("b/c")].into_iter().collect::<HashSet<PathBuf>>(),
		);
	}
}
