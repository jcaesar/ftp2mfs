use ipfs_api::IpfsClient;
use std::path::Path;
use std::io::prelude::*;

pub struct Mfs {
	ipfs: IpfsClient,
}
impl Mfs {
	pub fn new(api: &str) -> Mfs { Mfs {
		ipfs: ipfs_api::TryFromUri::from_str(api).unwrap(),
	}}
	pub async fn stat(&self, p: &Path)
		-> Result<ipfs_api::response::FilesStatResponse, ipfs_api::response::Error>
		{ self.ipfs.files_stat(p.to_str().unwrap()).await }
	pub async fn rm_r(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_rm(p.to_str().unwrap(), true).await }
	pub async fn rm(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_rm(p.to_str().unwrap(), false).await }
	pub async fn mkdirs(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_mkdir(p.to_str().unwrap(), true).await }
	pub async fn mkdir(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_mkdir(p.to_str().unwrap(), false).await }
	pub async fn mv(&self, s: &Path, d: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_mv(s.to_str().unwrap(), d.to_str().unwrap()).await }
	pub async fn cp(&self, s: &Path, d: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_cp(s.to_str().unwrap(), d.to_str().unwrap()).await }
	pub fn read(&self, s: &Path)
		-> impl futures_core::stream::Stream<Item = Result<bytes::Bytes, ipfs_api::response::Error>>
		{ self.ipfs.files_read(s.to_str().unwrap()) }

	pub async fn read_fully(&self, s: &Path) -> Result<Vec<u8>, ipfs_api::response::Error> {
		use futures_util::stream::TryStreamExt;
		self.read(s)
			.map_ok(|chunk| chunk.to_vec())
		    .try_concat()
			.await
		// Optimally, I'd have a version of this that returns a Read or similar…
	}
	pub async fn emplace<R: 'static + Read + Send + Sync>(&self, d: &Path, expected: usize, data: R)
		-> Result<(), ipfs_api::response::Error>
	{
		if expected < 1<<20 {
			self.ipfs.files_write(d.to_str().unwrap(), true, true, data).await?;
		} else {
			let added = self.ipfs.add(data).await?;
			self.ipfs.files_cp(&format!("/ipfs/{}", added.hash), d.to_str().unwrap()).await?;
			self.ipfs.pin_rm(&added.hash, true).await?;
		}
		Ok(())
	}
}
