use anyhow::{ Result, Context as _ };
use crate::suite::*;
use crate::nabla::{ FileInfo, SyncInfo };
use std::path::{ Path, PathBuf };
use ignore::gitignore::Gitignore;
use url::Url;
use futures::AsyncRead;
use std::collections::HashMap;
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
        let (client, files) = arrsync::RsyncClient::connect(self.source.clone()) // TODO: noclone
            .await.context("Connect to rsync server")?;
        self.client = Some(client);
        let mut ret = SyncInfo::new();
        let mut path_lookup = self.files.lock().unwrap();
        for file in files.into_iter().filter(arrsync::File::is_file) {
            let path = Path::new(std::str::from_utf8(&file.path).context("TODO: don't use Path")?);
            ret.files.insert(path.to_owned(), FileInfo {
                t: file.mtime,
                s: Some(file.size as usize),
                deleted: None
            });
            path_lookup.insert(path.to_owned(), file);
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
