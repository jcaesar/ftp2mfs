use ignore::gitignore::Gitignore;
use std::path::Path;
use futures::AsyncRead;
use crate::Settings;
use crate::Opts;
use crate::nabla::SyncInfo;
use anyhow::{ Result, Context };

#[async_trait::async_trait]
pub trait Provider {
	async fn get(&self, p: &Path) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>>;
	fn base(&self) -> &url::Url;
}


#[async_trait::async_trait]
pub trait Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>>;
	async fn recurse(&self, ignore: Gitignore) -> Result<SyncInfo>;
}

pub fn make(opts: &Opts, settings: &Settings) -> Result<Box<dyn Suite>> {
	let mut source = settings.source.clone();
	let default_username = if settings.source.scheme() == "ftp" { Some("anonymous") } else { None };
	for user in opts.user.as_deref().or(settings.user.as_deref()).or(default_username) {
		source.set_username(user).ok().context("Username into URL")?;
	}
	for pass in opts.pass.as_deref().or(settings.pass.as_deref()) {
		source.set_password(Some(pass)).ok().context("Password into URL")?;
	}
	match settings.source.scheme() {
		"ftp" => Ok(Box::new(crate::fromftp::Suite { source })),
		"file" => Ok(Box::new(crate::fromfile::Suite { source })),
        "http" | "https" => Ok(Box::new(crate::fromhttp::Suite::new(source))),
		_ => anyhow::bail!("Invalid source url {}: only ftp is supported", settings.source),
	}
}
