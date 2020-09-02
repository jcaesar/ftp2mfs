use anyhow::{ Result, Context as _ };
use crate::suite::*;
use ignore::gitignore::Gitignore;
use url::Url;
use crate::nabla::SyncInfo;
use futures::AsyncRead;
use semaphored::*;
use std::path::Path;

mod semaphored;
mod spider;

type Client = Semaphored<reqwest::Client>;

#[derive(Clone)]
pub struct Suite {
	source: Url,
}

impl Suite {
	pub fn new(source: Url) -> Suite { Suite { source } }
    fn client() -> Client {
	    let client = reqwest::Client::builder()
	    	.user_agent("jcaesar/ftp2mfs")
	    	.gzip(true)
	    	.brotli(true)
	    	.redirect(reqwest::redirect::Policy::none())
	    	.build().unwrap();
    	Semaphored::new(client, 8)
    }
}

#[async_trait::async_trait]
impl crate::suite::Suite for Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>> {
		Ok(Box::new(HttpProvider { base: self.source.clone(), client: Suite::client() }))
	}
	async fn recurse(&self, ignore: Gitignore) -> Result<SyncInfo> {
        spider::Spider::new(Suite::client(), self.source.clone(), ignore).run().await
	}
}

struct HttpProvider {
	base: Url,
    client: Client,
}

#[async_trait::async_trait]
impl Provider for HttpProvider {
    async fn get(&self, p: &Path) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        let url = self.base.join(p.to_str().unwrap())?;
        let ctx = format!("GET {}", &url);
        use futures_util::stream::{ StreamExt, TryStreamExt };
        let stream = self.client.acquire().await
            .get(url).send()
                .await.context(ctx)?
            .bytes_stream()
            .map(|e| e.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
            .into_async_read();
        Ok(Box::new(stream))
    }
    fn base(&self) -> &url::Url {
        &self.base
    }
}
