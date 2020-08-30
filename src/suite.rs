use ignore::gitignore::Gitignore;
use std::path::Path;
use futures::AsyncRead;
use crate::Settings;
use crate::Opts;
use crate::nabla::SyncInfo;
use anyhow::{ Result, Context };

pub trait Provider {
	fn get(&self, p: &Path) -> Box<dyn AsyncRead + Send + Sync + Unpin>;
	fn base(&self) -> &url::Url;
	fn log_and_get(&self, p: &Path) -> Box<dyn AsyncRead + Send + Sync + Unpin> {
		log::info!("Retrieving {}", self.base().join(p.to_str().unwrap()).unwrap().into_string());
		return self.get(p);
	}
}


#[async_trait::async_trait]
pub trait Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>>;
	async fn recurse(&self, ignore: Gitignore) -> Result<SyncInfo>;
}

pub fn make(opts: &Opts, settings: &Settings) -> Result<Box<dyn Suite>> {
	let mut source = settings.source.clone();
	source.set_username(opts.user.as_ref().unwrap_or(&settings.user))
		.ok().context("Username into URL")?;
	source.set_password(opts.pass.as_deref().or(settings.pass.as_deref()))
		.ok().context("Password into URL")?;
	match settings.source.scheme() {
		"ftp" => {
			Ok(Box::new(crate::fromftp::Suite { source }))
		},
		_ => anyhow::bail!("Invalid source url {}: only ftp is supported", settings.source),
	}
}
