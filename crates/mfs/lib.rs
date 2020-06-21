use ipfs_api::IpfsClient;
use std::path::{ Path, PathBuf };
use std::io::prelude::*;
use futures::stream::StreamExt;
use ipfs_api::response::Error as IpfsApiError;
use thiserror::Error as ThisError;
use ipfs_api::response::FilesStatResponse;

#[derive(ThisError, Debug)]
#[error("mfs: {msg}")]
pub struct OpError {
	pub msg: String,
	#[source]
	pub source: failure::Compat<IpfsApiError>,
}
type OpResult<T> = Result<T, OpError>;

trait OpText<T, E> {
	fn with_context<C, F>(self, f: F) -> Result<T, OpError>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C;
}
impl<T> OpText<T, ipfs_api::response::Error> for std::result::Result<T, ipfs_api::response::Error> {
	fn with_context<C, F>(self, f: F) -> Result<T, OpError>
	where
		C: std::fmt::Display + Send + Sync + 'static,
		F: FnOnce() -> C
	{ match self {
		Ok(ok) => Ok(ok),
		Err(e) => Err(OpError {
			msg: f().to_string(),
			source: failure::Fail::compat(e)
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
}
impl Mfs {
	pub fn new(api: &str) -> Result<Mfs, http::uri::InvalidUri> { Ok( Mfs {
		ipfs: ipfs_api::TryFromUri::from_str(api)?,
	})}
	pub async fn rm_r<P: AsRef<Path>>(&self, p: P) -> OpResult<()> { Ok(
		self.ipfs.files_rm(p.unpath(), true)
			.await.with_context(|| format!("rm -r {:?}", p.as_ref()))?
	)}
	pub async fn rm<P: AsRef<Path>>(&self, p: P) -> OpResult<()> { Ok(
		self.ipfs.files_rm(p.unpath(), false)
			.await.with_context(|| format!("rm {:?}", p.as_ref()))?
	)}
	pub async fn mkdirs<P: AsRef<Path>>(&self, p: P) -> OpResult<()> { Ok(
		self.ipfs.files_mkdir(p.unpath(), true)
			.await.with_context(|| format!("mkdir -p {:?}", p.as_ref()))?
	)}
	pub async fn mkdir<P: AsRef<Path>>(&self, p: P) -> OpResult<()> { Ok(
		self.ipfs.files_mkdir(p.unpath(), false)
			.await.with_context(|| format!("mkdir -p {:?}", p.as_ref()))?
	)}
	pub async fn mv<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> OpResult<()> { Ok(
		self.ipfs.files_mv(s.unpath(), d.unpath())
			.await.with_context(|| format!("mv {:?} {:?}", s.as_ref(), d.as_ref()))?
	)}
	pub async fn cp<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> OpResult<()> { Ok(
		self.ipfs.files_cp(s.unpath(), d.unpath())
			.await.with_context(|| format!("cp {:?} {:?}", s.as_ref(), d.as_ref()))?
	)}
	pub async fn ls<P: AsRef<Path>>(&self, p: P) -> OpResult<Vec<PathBuf>> { Ok(
		self.ipfs.files_ls(Some(p.unpath()))
			.await.with_context(|| format!("ls {:?}", p.as_ref()))?
			.entries
			.into_iter()
			.map(|e| e.name.into())
			.collect()
	)}
	pub async fn flush<P: AsRef<Path>>(&self, p: P) -> OpResult<()> { Ok(
		self.ipfs.files_flush(Some(p.unpath()))
			.await.with_context(|| format!("flush {:?}", p.as_ref()))?
	)}
	pub fn read<'a, P: AsRef<Path>>(&self, s: P) -> impl futures_core::stream::Stream<Item = OpResult<bytes::Bytes>> {
		self.ipfs.files_read(s.unpath()).map(move |e| Ok(e.with_context(|| format!("reading {:?}", s.as_ref()))?))
	}

	pub async fn read_fully<P: AsRef<Path>>(&self, s: P) -> OpResult<Vec<u8>> {
		use futures_util::stream::TryStreamExt;
		self.read(s)
			.map_ok(|chunk| chunk.to_vec())
			.try_concat()
			.await
		// Optimally, I'd have a version of this that returns a Read or similar…
	}
	pub async fn emplace<P: AsRef<Path>, R: 'static + Read + Send + Sync>(&self, d: P, expected: usize, data: R)
		-> OpResult<()>
	{
		let d = d.unpath();
		if expected < 1<<20 {
			self.ipfs.files_write(d, true, true, data)
			.await.with_context(|| format!("direct write to {:?}", d))?;
		} else {
			let ctx = |step: &'static str| move || format!("indirect write to {:?} failed when {}", d, step);
			let added = self.ipfs.add(data)
				.await.with_context(ctx("adding/pinning data normally (ipfs add) first"))?;
			self.ipfs.files_cp(&format!("/ipfs/{}", added.hash), d) // TODO: Use chcid when available
				.await.with_context(ctx("adding link to pinned data to ipfs"))?;
			self.ipfs.pin_rm(&added.hash, true)
				.await.with_context(ctx("removing pin"))?;
		}
		Ok(())
	}
	pub async fn stat<P: AsRef<Path>>(&self, p: P) -> OpResult<Option<FilesStatResponse>> {
		match self.ipfs.files_stat(p.unpath()).await {
			Ok(r) => return Ok(Some(r)),
			Err(ipfs_api::response::Error::Api(ipfs_api::response::ApiError { code: 0, .. })) => return Ok(None),
			e@Err(_) => e.with_context(|| format!("stat {:?}", p.as_ref()))?,
		};
		unreachable!("");
	}
}
