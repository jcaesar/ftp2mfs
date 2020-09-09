use anyhow::{ Result, Context as _ };
use crate::suite::*;
use crate::nabla::{ FileInfo, SyncInfo };
use std::path::{ Path, PathBuf };
use ignore::gitignore::Gitignore;
use url::Url;
use futures::AsyncRead;
use std::collections::{ HashMap, HashSet };
use std::sync::{ Arc, Mutex };

#[derive(Clone)]
pub struct Suite {
	pub source: Url,
    client: Option<arrsync::RsyncClient>,
    files: Arc<Mutex<HashMap<PathBuf, arrsync::File>>>,
}

impl Suite {
	pub fn new(source: Url) -> Suite { Suite { source, client: None, files: std::default::Default::default() } }
}

#[async_trait::async_trait]
impl crate::suite::Suite for Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>> {
		Ok(Box::new(self.clone()))
	}
	async fn recurse(&mut self, ignore: Gitignore) -> Result<SyncInfo> {
        let (client, files) = arrsync::RsyncClient::connect(&self.source)
            .await.context("Connect to rsync server")?;
        self.client = Some(client);
        let mut ret = SyncInfo::new();
        let mut path_lookup = self.files.lock().unwrap();
        let vu8path = |vu8| std::str::from_utf8(vu8).context("Non UTF-8 path").map(Path::new);
        let ignored_folders: HashSet<&Path> = files.iter()
            .filter(|file| file.is_directory())
            .map(|file| vu8path(&file.path))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|path| ignore.matched(path, true).is_ignore())
            .collect::<HashSet<_>>();
        for file in files.iter().filter(|f| f.is_file()) {
            let path = vu8path(&file.path)?;
            if ignore.matched(path, false).is_ignore() {
                continue;
            }
            if path.ancestors().any(|anc| ignored_folders.contains(anc)) {
                continue;
            }
            ret.files.insert(path.to_owned(), FileInfo {
                t: file.mtime,
                s: Some(file.size as usize),
                deleted: None
            });
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
		Ok(Box::new(self.client.as_ref().context("Connection never established")?.get(&file).await?.compat()))
	}
	fn base(&self) -> &Url { &self.source }
}