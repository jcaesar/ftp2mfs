use crate::nabla::*;
use ftp::FtpStream;
use std::path::{ Path, PathBuf };

pub struct Recursor<'a> {
	ftp: &'a mut FtpStream,
	base: PathBuf,
	result: SyncInfo,
}
impl <'a> Recursor <'a> {
	pub fn run(ftp: &'a mut FtpStream) -> SyncInfo {
		let wd = ftp.pwd().unwrap();
		let mut r = Recursor {
			ftp,
			base: Path::new(&wd).to_path_buf(),
			result: SyncInfo::new(), // Marks the sync start time
		};
		r.rec();
		r.result
	}
	fn rec(&mut self) {
		let pth = Path::new(&self.ftp.pwd().unwrap()).to_path_buf();
		match self.ftp.nlst(None) {
			Ok(lst) => for ref f in lst {
				match self.ftp.cwd(f) {
					Ok(()) => {
						self.rec();
						self.ftp.cdup().unwrap();
					},
					Err(_) => {
						let name = pth.clone().join(f);
						let name = pathdiff::diff_paths(name, self.base.clone()).unwrap();
						self.result.files.insert(name, FileInfo {
							t: self.ftp.mdtm(&f).ok().flatten(),
							s: self.ftp.size(&f).ok().flatten(),
							deleted: None,
						});
					},
				}
			},
			Err(e) => println!("ERR {:?} {:?}", pth, e)
		}
	}
}
