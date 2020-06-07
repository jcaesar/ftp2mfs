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
	pub fn new(api: &str) -> Result<Mfs> { Ok( Mfs {
		ipfs: ipfs_api::TryFromUri::from_str(api)?,
	})}
	pub async fn stat<P: AsRef<Path>>(&self, p: P) -> Result<ipfs_api::response::FilesStatResponse> { Ok(
		self.ipfs.files_stat(p.unpath())
			.await.with_context(|| format!("mfs: stat {:?}", p.as_ref()))?
	)}
	pub async fn rm_r<P: AsRef<Path>>(&self, p: P) -> Result<()> { Ok(
		self.ipfs.files_rm(p.unpath(), true)
			.await.with_context(|| format!("mfs: rm -r {:?}", p.as_ref()))?
	)}
	pub async fn rm<P: AsRef<Path>>(&self, p: P) -> Result<()> { Ok(
		self.ipfs.files_rm(p.unpath(), false)
			.await.with_context(|| format!("mfs: rm {:?}", p.as_ref()))?
	)}
	pub async fn mkdirs<P: AsRef<Path>>(&self, p: P) -> Result<()> { Ok(
		self.ipfs.files_mkdir(p.unpath(), true)
			.await.with_context(|| format!("mfs: mkdir -p {:?}", p.as_ref()))?
	)}
	pub async fn mkdir<P: AsRef<Path>>(&self, p: P) -> Result<()> { Ok(
		self.ipfs.files_mkdir(p.unpath(), false)
			.await.with_context(|| format!("mfs: mkdir -p {:?}", p.as_ref()))?
	)}
	pub async fn mv<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> Result<()> { Ok(
		self.ipfs.files_mv(s.unpath(), d.unpath())
			.await.with_context(|| format!("mfs: mv {:?} {:?}", s.as_ref(), d.as_ref()))?
	)}
	pub async fn cp<PS: AsRef<Path>, PD: AsRef<Path>>(&self, s: PS, d: PD) -> Result<()> { Ok(
		self.ipfs.files_cp(s.unpath(), d.unpath())
			.await.with_context(|| format!("mfs: cp {:?} {:?}", s.as_ref(), d.as_ref()))?
	)}
	pub async fn ls<P: AsRef<Path>>(&self, p: P) -> Result<Vec<FilesEntry>> { Ok(
		self.ipfs.files_ls(Some(p.unpath()))
			.await.with_context(|| format!("mfs: ls {:?}", p.as_ref()))?
			.entries
	)}
	pub async fn flush<P: AsRef<Path>>(&self, p: P) -> Result<()> { Ok(
		self.ipfs.files_flush(Some(p.unpath()))
			.await.with_context(|| format!("mfs: flush {:?}", p.as_ref()))?
	)}
	pub fn read<'a, P: AsRef<Path>>(&self, s: P) -> impl futures_core::stream::Stream<Item = Result<bytes::Bytes>> {
		self.ipfs.files_read(s.unpath()).map(move |e| Ok(e.with_context(|| format!("mfs: reading {:?}", s.as_ref()))?))
	}

	pub async fn read_fully<P: AsRef<Path>>(&self, s: P) -> Result<Vec<u8>> {
		use futures_util::stream::TryStreamExt;
		self.read(s)
			.map_ok(|chunk| chunk.to_vec())
			.try_concat()
			.await
		// Optimally, I'd have a version of this that returns a Read or similar…
	}
	pub async fn emplace<P: AsRef<Path>, R: 'static + Read + Send + Sync>(&self, d: P, expected: usize, data: R)
		-> Result<()>
	{
		let d = d.unpath();
		if expected < 1<<20 {
			self.ipfs.files_write(d, true, true, data)
			.await.with_context(|| format!("mfs: direct write to {:?}", d))?;
		} else {
			let ctx = |step: &'static str| move || format!("mfs: indirect write to {:?} failed when {}", d, step);
			let added = self.ipfs.add(data)
				.await.with_context(ctx("adding/pinning data normally (ipfs add) first"))?;
			self.ipfs.files_cp(&format!("/ipfs/{}", added.hash), d) // TODO: Use chcid when available
				.await.with_context(ctx("adding link to pinned data to ipfs"))?;
			self.ipfs.pin_rm(&added.hash, true)
				.await.with_context(ctx("removing pin"))?;
		}
		Ok(())
	}
	pub async fn exists<P: AsRef<Path>>(&self, p: P) -> Result<bool> {
		match self.ipfs.files_stat(p.unpath()).await {
			Ok(_) => return Ok(true),
			Err(ipfs_api::response::Error::Api(ipfs_api::response::ApiError { code: 0, .. })) => return Ok(false),
			e@Err(_) => e.with_context(|| format!("mfs: stat {:?}", p.as_ref()))?,
		};
		unreachable!("");
	}
}
