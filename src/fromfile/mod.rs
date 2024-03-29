use crate::nabla::{FileInfo, SyncInfo};
use crate::suite::*;
use anyhow::Result;
use futures::AsyncRead;
use ignore::gitignore::Gitignore;
use std::path::Path;
use url::Url;

#[derive(Clone)]
pub struct Suite {
	pub source: Url,
}

impl Suite {
	fn path(&self) -> &Path {
		Path::new(self.source.path())
	}
}

#[async_trait::async_trait]
impl crate::suite::Suite for Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>> {
		Ok(Box::new(self.clone()))
	}
	async fn recurse(&mut self, ignore: Gitignore, _solid: Gitignore) -> Result<SyncInfo> {
		// TODO: if this is an sshfs, considering _solid might be worth it.
		use walkdir::WalkDir;
		let rpath = |entry: &walkdir::DirEntry| {
			entry
				.path()
				.strip_prefix(&self.path())
				.expect(&format!(
					"File {:?} outside walkdir base path {:?}",
					entry.path(),
					&self.path()
				))
				.to_owned()
		};
		let walk = WalkDir::new(self.path())
			.follow_links(false)
			.into_iter()
			.filter_entry(|entry| {
				let meta = match entry.metadata() {
					Ok(meta) => meta,
					Err(_) => return true,
				};
				let rpath = rpath(entry);
				let ignore = ignore.matched(&rpath, meta.is_dir());
				match ignore.is_ignore() {
					true => log::debug!("{:?} ignored by {:?}", rpath, ignore),
					false => log::trace!("{:?} not ignored: {:?}", rpath, ignore),
				};
				!ignore.is_ignore()
			});
		let mut si = SyncInfo::new();
		for entry in walk {
			let entry = match entry {
				Err(e) => {
					log::warn!("Traversing local folder: {}", e);
					log::debug!("{:#?}", e);
					continue;
				}
				Ok(e) => e,
			};
			let meta = entry.metadata()?;
			let rpath = rpath(&entry);
			if meta.file_type().is_symlink() {
				let linktarget = match std::fs::read_link(entry.path()) {
					Err(e) => {
						log::warn!("Can't read link {:?}: {}", rpath, e);
						continue;
					}
					Ok(t) => t,
				};
				si.symlinks.insert(rpath.to_owned(), linktarget);
			} else if meta.is_file() {
				let modified = meta.modified()?.into();
				let size = meta.len() as usize;
				si.files
					.insert(rpath.to_path_buf(), FileInfo::found(Some(modified), Some(size)));
			}
		}
		Ok(si)
	}
}

#[async_trait::async_trait]
impl crate::suite::Provider for Suite {
	async fn get(&self, p: &Path) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
		use tokio_util::compat::TokioAsyncReadCompatExt;
		Ok(Box::new(
			tokio::fs::File::open(self.path().join(p)).await.unwrap().compat(),
		))
	}
	fn base(&self) -> &Url {
		&self.source
	}
}
