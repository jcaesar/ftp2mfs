use std::path::Path;
use futures::AsyncRead;

pub trait Provider {
	fn get(&self, p: &Path) -> Box<dyn AsyncRead + Send + Sync + Unpin>;
	fn base(&self) -> &url::Url;
	fn log_and_get(&self, p: &Path) -> Box<dyn AsyncRead + Send + Sync + Unpin> {
		log::info!("Retrieving {}", self.base().join(p.to_str().unwrap()).unwrap().into_string());
		return self.get(p);
	}
}
