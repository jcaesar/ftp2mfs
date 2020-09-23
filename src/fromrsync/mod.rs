use crate::nabla::{FileInfo, SyncInfo};
use crate::suite::*;
use anyhow::{Context as _, Result};
use futures::AsyncRead;
use ignore::gitignore::Gitignore;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use url::Url;

#[derive(Clone)]
pub struct Suite {
	pub source: Url,
	client: Option<arrsync::RsyncClient>,
	files: Arc<Mutex<HashMap<PathBuf, arrsync::File>>>,
}

impl Suite {
	pub fn new(source: Url) -> Suite {
		Suite {
			source,
			client: None,
			files: std::default::Default::default(),
		}
	}
}

#[async_trait::async_trait]
impl crate::suite::Suite for Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>> {
		Ok(Box::new(self.clone()))
	}
	async fn recurse(&mut self, ignore: Gitignore) -> Result<SyncInfo> {
		let (client, files) = arrsync::RsyncClient::connect(&self.source)
			.await
			.context("Connect to rsync server")?;
		self.client = Some(client);
		let mut ret = SyncInfo::new();
		let mut path_lookup = self.files.lock().unwrap();
		let vu8path = |vu8| std::str::from_utf8(vu8).context("Non UTF-8 path").map(Path::new);
		let ignored_folders: HashSet<&Path> = files
			.iter()
			.filter(|file| file.is_directory())
			.map(|file| vu8path(&file.path))
			.collect::<Result<Vec<_>, _>>()?
			.into_iter()
			.filter(|path| ignore.matched(path, true).is_ignore())
			.collect::<HashSet<_>>();
		for file in files.iter() {
			let path = vu8path(&file.path)?;
			if ignore.matched(path, file.is_directory()).is_ignore() {
				log::debug!("{:?} ignored", path);
				continue;
			}
			if path.ancestors().any(|anc| ignored_folders.contains(anc)) {
				log::debug!("{:?} in ignored folder", path);
				continue;
			}
			if file.is_file() {
				ret.files.insert(
					path.to_owned(),
					FileInfo {
						t: file.mtime,
						s: Some(file.size as usize),
						deleted: None,
					},
				);
			}
			if file.is_symlink() {
				ret.symlinks.insert(
					path.to_owned(),
					vu8path(file.symlink.as_ref().expect("symlink, no target"))?.to_owned(),
				);
			}
			path_lookup.insert(path.to_owned(), file.clone());
		}
		Ok(ret)
	}
}

#[async_trait::async_trait]
impl crate::suite::Provider for Suite {
	async fn get(&self, p: &Path) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
		let file = {
			let files = self.files.lock().unwrap();
			let file = files.get(p).context("No such file on server")?;
			file.clone()
		};
		use tokio_util::compat::Tokio02AsyncReadCompatExt;
		Ok(Box::new(
			self.client
				.as_ref()
				.context("Connection never established")?
				.get(&file)
				.await?
				.compat(),
		))
	}
	fn base(&self) -> &Url {
		&self.source
	}
}
