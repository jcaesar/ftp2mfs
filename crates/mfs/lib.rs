//! Small wrapper crate for `ipfs_api`.
//! Mostly exists because I do not want any part of the `failure` crate to appear in my code.
//!
//! Quick example:
//! ```no_run
//! # use mfs::Mfs;
//! # use std::path::Path;
//! # async {
//! let mfs = Mfs::new("http://127.0.0.1:5001").unwrap();
//! mfs.put(Path::new("/demo"), futures::io::Cursor::new("Hello ipfs files")).await.ok();
//! # };
//! ```
//!
//! The relevant functions are exposted through the main `Mfs` struct.

use futures::stream::StreamExt;
use futures_util::io::AsyncReadExt;
use ipfs_api::request::*;
use ipfs_api::response::Error as IpfsApiError;
use ipfs_api::response::FilesStatResponse;
use ipfs_api::IpfsClient;
use std::future::Future;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum MfsError {
	#[error("mfs: {msg}")]
	OpError {
		msg: String,
		#[source]
		source: failure::Compat<IpfsApiError>,
	},
	#[error("mfs: {msg}")]
	LayeredError {
		msg: String,
		#[source]
		source: Box<MfsError>,
	},
	#[error("mfs: could not read non-ipfs input")]
	InputError {
		#[source]
		source: std::io::Error,
	},
}

type MfsResult<T> = Result<T, MfsError>;

trait OpText<T, E> {
	fn with_context<C, F>(self, f: F) -> Result<T, MfsError>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C;
}
impl<T> OpText<T, ipfs_api::response::Error> for std::result::Result<T, ipfs_api::response::Error> {
	fn with_context<C, F>(self, f: F) -> Result<T, MfsError>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C,
	{
		match self {
			Ok(ok) => Ok(ok),
			Err(e) => Err(MfsError::OpError {
				msg: f().to_string(),
				source: failure::Fail::compat(e),
			}),
		}
	}
}
impl<T> OpText<T, MfsError> for std::result::Result<T, MfsError> {
	fn with_context<C, F>(self, f: F) -> Result<T, MfsError>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C,
	{
		match self {
			Ok(ok) => Ok(ok),
			Err(e) => Err(MfsError::LayeredError {
				msg: f().to_string(),
				source: Box::new(e),
			}),
		}
	}
}

trait Unpath {
	fn unpath(&self) -> &str;
}

impl<T> Unpath for T
where
	T: AsRef<Path>,
{
	fn unpath(&self) -> &str {
		self.as_ref().to_str().unwrap()
	}
}

/// The main struct
///
/// All of its functions will panic if the passed paths are not valid unicode.
#[derive(Clone)]
pub struct Mfs {
	ipfs: IpfsClient,
	/// Default hash function to use for write / create operations.
	pub hash_default: String,
	/// Default cid version to request on write / create operations. Defaults to `1`.
	pub cid_default: i32,
	/// Whether to write files with `raw-leaves`
	pub raw_leaves_default: bool,
}
impl Mfs {
	pub fn new(api: &str) -> Result<Mfs, http::uri::InvalidUri> {
		Ok(Mfs {
			ipfs: ipfs_api::TryFromUri::from_str(api)?,
			hash_default: "sha2-256".to_owned(),
			cid_default: 1,
			raw_leaves_default: true,
		})
	}

	/// Remove file or folder (possibly non-empty)
	pub async fn rm_r<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> {
		Ok(self
			.ipfs
			.files_rm(p.unpath(), true)
			.await
			.with_context(|| format!("rm -r {:?}", p.as_ref()))?)
	}

	/// Remove file
	pub async fn rm<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> {
		Ok(self
			.ipfs
			.files_rm(p.unpath(), false)
			.await
			.with_context(|| format!("rm {:?}", p.as_ref()))?)
	}

	/// Create directory `p` and parents as needed.
	pub async fn mkdirs<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> {
		Ok(self
			.ipfs
			.files_mkdir_with_options(
				FilesMkdir::builder()
					.path(p.unpath())
					.parents(true)
					.cid_version(self.cid_default)
					.hash(&self.hash_default)
					.build(),
			)
			.await
			.with_context(|| format!("mkdir -p {:?}", p.as_ref()))?)
	}

	/// Create directory `p`. Requires that its parent already exist.
	pub async fn mkdir<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> {
		Ok(self
			.ipfs
			.files_mkdir_with_options(
				FilesMkdir::builder()
					.path(p.unpath())
					.parents(false)
					.cid_version(self.cid_default)
					.hash(&self.hash_default)
					.build(),
			)
			.await
			.with_context(|| format!("mkdir {:?}", p.as_ref()))?)
	}

	/// Rename / move `s` to `d`.
	///
	pub async fn mv<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> MfsResult<()> {
		Ok(self
			.ipfs
			.files_mv(s.unpath(), d.unpath())
			.await
			.with_context(|| format!("mv {:?} {:?}", s.as_ref(), d.as_ref()))?)
	}

	/// Copy path `s` to path `d`. Beware of `s` starting with `/ipfs` or `/ipns`.
	pub async fn cp<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> MfsResult<()> {
		Ok(self
			.ipfs
			.files_cp(s.unpath(), d.unpath())
			.await
			.with_context(|| format!("cp {:?} {:?}", s.as_ref(), d.as_ref()))?)
	}

	/// List files in folder
	pub async fn ls<P: AsRef<Path>>(&self, p: P) -> MfsResult<Vec<PathBuf>> {
		Ok(self
			.ipfs
			.files_ls(Some(p.unpath()))
			.await
			.with_context(|| format!("ls {:?}", p.as_ref()))?
			.entries
			.into_iter()
			.map(|e| e.name.into())
			.collect())
	}

	/// Read file at `s`.
	pub fn get<'a, P: AsRef<Path>>(
		&self,
		s: P,
	) -> impl futures_core::stream::Stream<Item = MfsResult<bytes::Bytes>> {
		self.ipfs
			.files_read(s.unpath())
			.map(move |e| Ok(e.with_context(|| format!("reading {:?}", s.as_ref()))?))
	}

	/// Read file at `s` into in-memory buffer.
	pub async fn get_fully<P: AsRef<Path>>(&self, s: P) -> MfsResult<Vec<u8>> {
		use futures_util::stream::TryStreamExt;
		self.get(s).map_ok(|chunk| chunk.to_vec()).try_concat().await
		// Optimally, I'd have a version of this that returns a Read or similar…
	}

	/// Flush folder
	pub async fn flush<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> {
		Ok(self
			.ipfs
			.files_flush(Some(p.unpath()))
			.await
			.with_context(|| format!("flush {:?}", p.as_ref()))?)
	}

	/// Write file to `d`.
	///
	/// Internally buffers chunks of 8 MiB and writes them with separate calls to IPFS
	/// due to limitations of `multipart`.
	pub async fn put<P: AsRef<Path>, R: 'static + futures::AsyncRead + Send + Sync + Unpin>(
		&self,
		d: P,
		mut data: R,
	) -> MfsResult<()> {
		let d = d.unpath();
		let mut firstwrite = true;
		let mut total = 0;
		let mut finished = false;
		let mut pending: Option<Pin<Box<dyn Future<Output = Result<(), ipfs_api::response::Error>> + Send>>> =
			None;
		while !finished {
			let mut buf = Vec::new();
			buf.resize(1 << 23, 0);
			let mut offset = 0;
			while offset < buf.len() && !finished {
				let read = data
					.read(&mut buf[offset..])
					.await
					.map_err(|e| MfsError::InputError { source: e })?;
				offset += read;
				finished = read == 0;
			}
			buf.truncate(offset);
			if offset == 0 && !firstwrite {
				break;
			}
			let req = self.ipfs.files_write_with_options(
				FilesWrite::builder()
					.path(d)
					.create(firstwrite)
					.truncate(firstwrite)
					.parents(true)
					.offset(total as i64)
					.count(offset as i64)
					.raw_leaves(self.raw_leaves_default)
					.cid_version(self.cid_default)
					.hash(&self.hash_default)
					.flush(true)
					.build(),
				Cursor::new(buf),
			);
			if let Some(req) = pending {
				req.await.with_context(|| format!("write {:?}: chunk", d))?;
			}
			pending = Some(Box::pin(req));
			total += offset;
			firstwrite = false;
		}
		if let Some(req) = pending {
			req.await.with_context(|| format!("write {:?}: final chunk", d))?;
		}
		let stat = self.stat(d).await.with_context(|| format!("write {:?}: confirm", d))?;
		if stat.map(|stat| stat.size != total as u64).unwrap_or(false) {
			self.rm(d).await.ok();
			todo!(
				"write {:?}: read/write sizes do not match - lost bytes :( TODO: don't panic",
				d
			);
		}
		Ok(())
	}
	/// Request hash, type, sizes, and block count of `p`. Returns `None` if the file does not exist
	pub async fn stat<P: AsRef<Path>>(&self, p: P) -> MfsResult<Option<FilesStatResponse>> {
		match self.ipfs.files_stat(p.unpath()).await {
			Ok(r) => return Ok(Some(r)),
			Err(ipfs_api::response::Error::Api(ipfs_api::response::ApiError { code: 0, .. })) => return Ok(None),
			e @ Err(_) => e.with_context(|| format!("stat {:?}", p.as_ref()))?,
		};
		unreachable!("");
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	#[ignore]
	async fn eierlegendewollmilchsau() {
		let mfs = Mfs::new("http://127.0.0.1:5001").expect("create client");
		let basedir = Path::new("/test-rust-ipfs-mfs");
		if mfs
			.stat(basedir)
			.await
			.expect("Test preparation: check existing files at test path")
			.is_some()
		{
			mfs.rm_r(basedir)
				.await
				.expect("Test preparation: remove already existing test files");
		}
		mfs.mkdir(basedir)
			.await
			.expect("Test preparation: make working directory");

		mfs.mkdir(basedir.join("a/b"))
			.await
			.err()
			.expect("mkdir does not create parents");
		mfs.mkdirs(basedir.join("a/b")).await.expect("mkdirs creates parents");
		let stat1 = mfs
			.stat(basedir)
			.await
			.expect("Statting working directory")
			.expect("Working directory exists");
		mfs.cp(basedir, basedir.join("a/c")).await.expect("cp succeeds");
		assert_eq!(
			mfs.stat(basedir.join("a/c")).await.unwrap().unwrap().hash,
			stat1.hash,
			"After cp is before cp (the hash)"
		);
		mfs.mv(basedir.join("a/b"), basedir.join("a/d"))
			.await
			.expect("mv succeeds");
		let mut ls1 = mfs.ls(basedir.join("a")).await.expect("Listing a");
		ls1.sort();
		assert_eq!(
			vec![PathBuf::from("c"), PathBuf::from("d")],
			ls1,
			"Directory listing matches expected"
		);

		for size in vec![0 as usize, 10, 1, 8 << 20, 9 << 20] {
			let f = &basedir.join("f");
			let data = (0..size).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
			mfs.put(f, futures::io::Cursor::new(data.clone()))
				.await
				.expect(&format!("Write file of size {}", size));
			let redata = mfs.get_fully(f).await.expect("Read file");
			assert_eq!(data.len(), redata.len(), "Read size matches written size");
			assert_eq!(data, redata, "Read matches written");
		}

		mfs.rm_r(basedir).await.expect("cleanup");
	}

	// Compile only test
	#[allow(dead_code)]
	fn good_little_future() {
		fn good<T: Send>(_: T) {}
		let mfs = Mfs::new("http://127.0.0.1:5001").expect("create client");
		let a = Path::new("");
		let b = a;
		let dat = futures::io::Cursor::new("asdf".as_bytes());
		good(mfs.put(a, dat));
		good(mfs.cp(a, b));
		good(mfs.mv(a, b));
		good(mfs.get(a));
		good(mfs.ls(a));
		good(mfs.mkdir(a));
		good(mfs.mkdirs(a));
		good(mfs.get_fully(a));
	}
}
