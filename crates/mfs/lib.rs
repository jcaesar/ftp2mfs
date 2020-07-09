use ipfs_api::IpfsClient;
use std::path::{ Path, PathBuf };
use futures::stream::StreamExt;
use ipfs_api::response::Error as IpfsApiError;
use thiserror::Error as ThisError;
use ipfs_api::response::FilesStatResponse;
use futures_util::io::AsyncReadExt;
use std::io::Cursor;
use std::future::Future;
use std::pin::Pin;

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
		F: FnOnce() -> C
	{ match self {
		Ok(ok) => Ok(ok),
		Err(e) => Err(MfsError::OpError {
			msg: f().to_string(),
			source: failure::Fail::compat(e)
		})
	}}
}
impl<T> OpText<T, MfsError> for std::result::Result<T, MfsError> {
	fn with_context<C, F>(self, f: F) -> Result<T, MfsError>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C
	{ match self {
		Ok(ok) => Ok(ok),
		Err(e) => Err(MfsError::LayeredError {
			msg: f().to_string(),
			source: Box::new(e)
		})
	}}
}

trait Unpath {
	fn unpath(&self) -> &str;
}

impl<T> Unpath for T where T: AsRef<Path> {
	fn unpath(&self) -> &str { self.as_ref().to_str().unwrap() }
}

pub struct Mfs {
	ipfs: IpfsClient,
	pub flush_default: bool,
	pub hash_default: Option<String>,
	pub cid_default: i32,
	pub raw_leaves_default: bool,
}
impl Mfs {
	pub fn new(api: &str) -> Result<Mfs, http::uri::InvalidUri> { Ok( Mfs {
		ipfs: ipfs_api::TryFromUri::from_str(api)?,
		flush_default: true,
		hash_default: None,
		cid_default: 1,
		raw_leaves_default: true,
	})}
	pub async fn rm_r<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> { Ok(
		self.ipfs.files_rm(p.unpath(), true, self.flush_default)
			.await.with_context(|| format!("rm -r {:?}", p.as_ref()))?
	)}
	pub async fn rm<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> { Ok(
		self.ipfs.files_rm(p.unpath(), false, self.flush_default)
			.await.with_context(|| format!("rm {:?}", p.as_ref()))?
	)}
	pub async fn mkdirs<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> { Ok(
		self.ipfs.files_mkdir(p.unpath(), true, self.cid_default, self.hash_default.as_deref(), self.flush_default)
			.await.with_context(|| format!("mkdir -p {:?}", p.as_ref()))?
	)}
	pub async fn mkdir<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> { Ok(
		self.ipfs.files_mkdir(p.unpath(), false, self.cid_default, self.hash_default.as_deref(), self.flush_default)
			.await.with_context(|| format!("mkdir -p {:?}", p.as_ref()))?
	)}
	pub async fn mv<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> MfsResult<()> { Ok(
		self.ipfs.files_mv(s.unpath(), d.unpath(), self.flush_default)
			.await.with_context(|| format!("mv {:?} {:?}", s.as_ref(), d.as_ref()))?
	)}
	pub async fn cp<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> MfsResult<()> { Ok(
		self.ipfs.files_cp(s.unpath(), d.unpath(), self.flush_default)
			.await.with_context(|| format!("cp {:?} {:?}", s.as_ref(), d.as_ref()))?
	)}
	pub async fn ls<P: AsRef<Path>>(&self, p: P) -> MfsResult<Vec<PathBuf>> { Ok(
		self.ipfs.files_ls(Some(p.unpath()), false)
			.await.with_context(|| format!("ls {:?}", p.as_ref()))?
			.entries
			.into_iter()
			.map(|e| e.name.into())
			.collect()
	)}
	pub async fn flush<P: AsRef<Path>>(&self, p: P) -> MfsResult<()> { Ok(
		self.ipfs.files_flush(Some(p.unpath()))
			.await.with_context(|| format!("flush {:?}", p.as_ref()))?
	)}
	pub fn get<'a, P: AsRef<Path>>(&self, s: P) -> impl futures_core::stream::Stream<Item = MfsResult<bytes::Bytes>> {
		self.ipfs.files_read(s.unpath(), 0, None).map(move |e| Ok(e.with_context(|| format!("reading {:?}", s.as_ref()))?))
	}

	pub async fn get_fully<P: AsRef<Path>>(&self, s: P) -> MfsResult<Vec<u8>> {
		use futures_util::stream::TryStreamExt;
		self.get(s)
			.map_ok(|chunk| chunk.to_vec())
			.try_concat()
			.await
		// Optimally, I'd have a version of this that returns a Read or similar…
	}
	pub async fn put<P: AsRef<Path>, R: 'static + futures::AsyncRead + Send + Sync + Unpin>(&self, d: P, mut data: R)
		-> MfsResult<()>
	{
		let d = d.unpath();
		let mut firstwrite = true;
		let mut total = 0;
		let mut finished = false;
		let mut pending: Option<Pin<Box<dyn Future<Output = Result<(), ipfs_api::response::Error>>>>> = None;
		while !finished {
			let mut buf = Vec::new();
			buf.resize(1 << 23, 0);
			let mut offset = 0;
			while offset < buf.len() && !finished {
				let read = data.read(&mut buf[offset..])
					.await.map_err(|e| MfsError::InputError { source: e })?;
				offset += read;
				finished = read == 0;
			}
			buf.truncate(offset);
			if finished && !firstwrite {
				break;
			}
			let req = self.ipfs.files_write(d,
				firstwrite, firstwrite, true,
				total as i64, Some(offset as i64),
				self.raw_leaves_default, self.cid_default, self.hash_default.as_deref(), self.flush_default,
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
		let stat = self.stat(d)
			.await.with_context(|| format!("write {:?}: confirm", d))?;
		if stat.map(|stat| stat.size != total as u64).unwrap_or(false) {
			self.rm(d).await.ok();
			panic!("write {:?}: read/write sizes do not match - lost bytes :( TODO: don't panic");
		}
		Ok(())
	}
	pub async fn stat<P: AsRef<Path>>(&self, p: P) -> MfsResult<Option<FilesStatResponse>> {
		match self.ipfs.files_stat(p.unpath(), false).await {
			Ok(r) => return Ok(Some(r)),
			Err(ipfs_api::response::Error::Api(ipfs_api::response::ApiError { code: 0, .. })) => return Ok(None),
			e@Err(_) => e.with_context(|| format!("stat {:?}", p.as_ref()))?,
		};
		unreachable!("");
	}
}
