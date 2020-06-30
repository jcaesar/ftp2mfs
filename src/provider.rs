use std::io::prelude::*;
use std::path::Path;

pub trait Provider {
	fn get(&self, p: &Path) -> Box<dyn Read + Send + Sync>;
	fn base(&self) -> &url::Url;
	fn log_and_get(&self, p: &Path) -> Box<dyn Read + Send + Sync> {
		log::info!("Retrieving {}", self.base().join(p.to_str().unwrap()).unwrap().into_string());
		return self.get(p);
	}
}
