use anyhow::{ Result, Context as _ };
use crate::suite::*;
use crate::nabla::{ FileInfo, SyncInfo };
use std::path::{ Path, PathBuf };
use ignore::gitignore::Gitignore;
use url::Url;
use futures::AsyncRead;
use std::pin::Pin;
use std::task::{ Poll, Context };

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
	async fn recurse(&self, ignore: Gitignore) -> Result<SyncInfo> {
		use walkdir::WalkDir;
		let nab = |entry: Result<walkdir::DirEntry, walkdir::Error>| -> Result<Option<(PathBuf, FileInfo)>> {
			let entry = entry.context("Traverse directory")?;
			let meta = entry.metadata()?;
			let path = entry.path();
			let path = path.strip_prefix(&self.path())
				.with_context(|| format!("File {:?} outside base path {:?}", path, &self.path()))?;
			if meta.is_dir() || ignore.matched(path, false).is_ignore() { return Ok(None) } // TODO: Unsure whether this is correct
			let modified = meta.modified()?.into();
			let size = meta.len() as usize;
			Ok(Some((path.to_path_buf(), FileInfo { t: Some(modified), s: Some(size), deleted: None })))
		};
		let fi = WalkDir::new(self.path())
			.follow_links(true)
			.into_iter()
			.filter_map(|e|
				nab(e).map_err(|e| {
					log::warn!("Traversing local folder: {}", e);
					log::debug!("{:#?}", e);
				}).ok().flatten()
			)
			.collect();
		let mut si = SyncInfo::new();
		si.files = fi;
		Ok(si)
	}
}

#[async_trait::async_trait]
impl crate::suite::Provider for Suite {
	async fn get(&self, p: &Path) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
		Ok(Box::new(Argh(tokio::fs::File::open(self.path().join(p)).await.unwrap())))
	}
	fn base(&self) -> &Url { &self.source }
}

// You see, I thought I'd get away without implementing another AsyncRead.
// But no, I need a futures::Async read, but I have a tokio_io::AsyncRead, but I need a futures_io::AsyncRead
// Le sigh.
struct Argh(tokio::fs::File);
impl AsyncRead for Argh {
	fn poll_read(self: Pin<&mut Self>, ctx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, std::io::Error>> {
		tokio::io::AsyncRead::poll_read(Pin::new(&mut self.get_mut().0), ctx, buf)
	}
	// ok, it wasn't so bad
}
