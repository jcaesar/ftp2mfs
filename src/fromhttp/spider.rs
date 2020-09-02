use url::Url;
use crate::nabla::{ FileInfo, SyncInfo };
use select::document::Document;
use select::predicate;
use std::path::Path;
use reqwest::header::*;
use std::pin::Pin;
use std::sync::Arc;
use anyhow::{ Result, Context };
use super::semaphored::*;
use ignore::gitignore::Gitignore;
use std::path::PathBuf;

pub struct Spider {
    client: Semaphored<reqwest::Client>,
    ignore: Gitignore,
    base: Url,
}

impl Spider {
    pub fn new(client: Semaphored<reqwest::Client>, base: Url, ignore: Gitignore) -> Arc<Spider> {
        Arc::new(Spider { client, ignore, base })
    }
    pub async fn run(self: Arc<Spider>) -> Result<SyncInfo> {
        let url = self.base.clone();
        let files = self.rec(url).await?;
        let mut ret = SyncInfo::new();
        ret.files = files.into_iter().collect();
        return Ok(ret);
    }
    fn rec(self: Arc<Spider>, url: Url)
        -> Pin<Box<dyn std::future::Future<Output = Result<Vec<(PathBuf, FileInfo)>>> + Send + 'static>>
    { Box::pin(async move {	
        let mut jobs = vec![];
        for r in refs_in(&*self.client.acquire().await, url.clone()).await?.into_iter() {
            let sub = match url.join(&r) {
                Ok(sub) => sub,
                Err(_) => {
                    log::info!("Ignoring odd link {} in {}", r, url);
                    continue;
                }
            };
            if !sub.has_authority() || sub.scheme() != url.scheme() || sub.host() != url.host() {
                log::debug!("{} linked to unrelated {}", &url, sub);
                continue;
            }
            if sub.fragment().is_some() || sub.query().is_some() {
                log::debug!("Ignoring parametrized url {}", sub);
                continue;
            }
            let path = Path::new(sub.path());
            let base = Path::new(url.path());
            if base.starts_with(path) {
                log::debug!("Ignoring link from {} to parent {}", url, sub);
                continue;
            }
            if path.parent() != Some(base) {
                log::info!("Ignoring link from {} to {} (not a direct child)", url, sub);
                continue;
            }
            let is_folder = sub.path().ends_with("/");
            let rel = Path::new(sub.path()).strip_prefix(self.base.path())?.to_path_buf();
            let fakeabs = Path::new("/").join(&rel);
            let ignore = self.ignore.matched(&fakeabs, is_folder);
	        log::debug!("Ignore? {} checked as {:?}: {:?}", sub, fakeabs, ignore);
	        if ignore.is_ignore() {
                continue;
            }
            let itsame = self.clone();
            if is_folder {
                jobs.push(tokio::spawn(itsame.rec(sub)));
            } else {
                jobs.push(tokio::spawn(itsame.get_meta(sub, rel)));
            }
        }
        let mut ret = vec![];
        for job in jobs.iter_mut() {
            for res in job.await??.into_iter() {
                ret.push(res);
            }
        }
        Ok(ret)
    })}

    async fn get_meta(self: Arc<Spider>, url: Url, rel: PathBuf) -> Result<Vec<(PathBuf, FileInfo)>> {
        let headers = self.client.acquire().await
            .head(url.clone()).send()
            .await.context(format!("HEADing {}", url))?;
        let headers = headers.headers();
        let lm: Option<chrono::DateTime<chrono::Utc>> = headers
            .get(LAST_MODIFIED)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| chrono::DateTime::parse_from_rfc2822(h)
                .map(|t| t.into())
                .map_err(|e| log::trace!("Date parse error {:?}", e))
                .ok());
        if lm.is_none() {
            log::warn!("No parseable modification date on {} ({:?})", url, headers.get(LAST_MODIFIED));
        }
        let sz: Option<usize> = headers
            .get(CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse().ok());
        let res = (rel, FileInfo { t: lm, s: sz, deleted: None });
        log::debug!("Found {:?}", res);
        Ok(vec![res])
    }
}

async fn refs_in(client: &reqwest::Client, url: Url) -> Result<Vec<String>> {
	log::debug!("Searching {}", url);
	let req = client.get(url).send().await?;
	let bytes = std::io::Cursor::new(req.bytes().await?);
	let doc = Document::from_read(bytes)?;
	Ok(doc.find(predicate::Name("a")).filter_map(|n| n.attr("href").map(|s| s.to_owned())).collect())
}
