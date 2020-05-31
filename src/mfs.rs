use ipfs_api::IpfsClient;
use std::path::Path;
use std::io::prelude::*;
use futures::stream::StreamExt;
pub use ipfs_api::response::FilesEntry;
pub use anyhow::Result; // Should really use thiserror…

// You can't use anyhow::Context while using failure::ResultExt, but ResultExt's context doesn't preserve the backtrace.
trait IvonneText<T, E> {
	fn with_context<C, F>(self, f: F) -> Result<T, anyhow::Error>
    where
        C: std::fmt::Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}
impl<T> IvonneText<T, ipfs_api::response::Error> for std::result::Result<T, ipfs_api::response::Error> {
	fn with_context<C, F>(self, f: F) -> Result<T, anyhow::Error>
    where
        C: std::fmt::Display + Send + Sync + 'static,
        F: FnOnce() -> C
	{ anyhow::Context::with_context(failure::ResultExt::compat(self), f) }
}

pub struct Mfs {
	ipfs: IpfsClient,
}
impl Mfs {
	pub fn new(api: &str) -> Result<Mfs> { Ok( Mfs {
		ipfs: ipfs_api::TryFromUri::from_str(api)?,
	})}
	#[allow(dead_code)]
	pub async fn stat(&self, p: &Path) -> Result<ipfs_api::response::FilesStatResponse> { Ok(
		self.ipfs.files_stat(p.to_str().unwrap())
			.await.with_context(|| format!("mfs: stat {:?}", p))?
	)}
	pub async fn rm_r(&self, p: &Path) -> Result<()> { Ok(
		self.ipfs.files_rm(p.to_str().unwrap(), true)
			.await.with_context(|| format!("mfs: rm -r {:?}", p))?
	)}
	pub async fn rm(&self, p: &Path) -> Result<()> { Ok(
		self.ipfs.files_rm(p.to_str().unwrap(), false)
			.await.with_context(|| format!("mfs: rm {:?}", p))?
	)}
	pub async fn mkdirs(&self, p: &Path) -> Result<()> { Ok(
		self.ipfs.files_mkdir(p.to_str().unwrap(), true)
			.await.with_context(|| format!("mfs: mkdir -p {:?}", p))?
	)}
	pub async fn mkdir(&self, p: &Path) -> Result<()> { Ok(
		self.ipfs.files_mkdir(p.to_str().unwrap(), false)
			.await.with_context(|| format!("mfs: mkdir -p {:?}", p))?
	)}
	pub async fn mv(&self, s: &Path, d: &Path) -> Result<()> { Ok(
		self.ipfs.files_mv(s.to_str().unwrap(), d.to_str().unwrap())
			.await.with_context(|| format!("mfs: mv {:?} {:?}", s, d))?
	)}
	pub async fn cp(&self, s: &Path, d: &Path) -> Result<()> { Ok(
		self.ipfs.files_cp(s.to_str().unwrap(), d.to_str().unwrap())
			.await.with_context(|| format!("mfs: cp {:?} {:?}", s, d))?
	)}
	pub async fn ls(&self, p: &Path) -> Result<Vec<FilesEntry>> { Ok(
		self.ipfs.files_ls(Some(p.to_str().unwrap()))
			.await.with_context(|| format!("mfs: ls {:?}", p))?
			.entries
	)}
	pub fn read<'a>(&self, s: &'a Path) -> impl futures_core::stream::Stream<Item = Result<bytes::Bytes>> + 'a {
		self.ipfs.files_read(s.to_str().unwrap()).map(move |e| Ok(e.with_context(|| format!("mfs: reading {:?}", s))?))
	}

	pub async fn read_fully(&self, s: &Path) -> Result<Vec<u8>> {
		use futures_util::stream::TryStreamExt;
		self.read(s)
			.map_ok(|chunk| chunk.to_vec())
			.try_concat()
			.await
		// Optimally, I'd have a version of this that returns a Read or similar…
	}
	pub async fn emplace<R: 'static + Read + Send + Sync>(&self, d: &Path, expected: usize, data: R)
		-> Result<()>
	{
		if expected < 1<<20 {
			self.ipfs.files_write(d.to_str().unwrap(), true, true, data)
			.await.with_context(|| format!("mfs: direct write to {:?}", d))?;
		} else {
			let ctx = |step: &'static str| move || format!("mfs: indirect write to {:?} failed when {}", d, step);
			let added = self.ipfs.add(data)
				.await.with_context(ctx("adding/pinning data normally (ipfs add) first"))?;
			self.ipfs.files_cp(&format!("/ipfs/{}", added.hash), d.to_str().unwrap()) // TODO: Use chcid when available
				.await.with_context(ctx("adding link to pinned data to ipfs"))?;
			self.ipfs.pin_rm(&added.hash, true)
				.await.with_context(ctx("removing pin"))?;
		}
		Ok(())
	}
	pub async fn exists(&self, p: &Path) -> Result<bool> {
		match self.ipfs.files_stat(p.to_str().unwrap()).await {
			Ok(_) => return Ok(true),
			Err(ipfs_api::response::Error::Api(ipfs_api::response::ApiError { code: 0, .. })) => return Ok(false),
			e@Err(_) => e.with_context(|| format!("mfs: stat {:?}", p))?,
		};
		unreachable!("");
	}
}
