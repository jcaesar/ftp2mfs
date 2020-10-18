use crate::nabla::SyncInfo;
use crate::Opts;
use crate::Settings;
use anyhow::{Context, Result};
use futures::AsyncRead;
use ignore::gitignore::Gitignore;
use std::path::Path;

#[async_trait::async_trait]
pub trait Provider {
	async fn get(&self, p: &Path) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>>;
	fn base(&self) -> &url::Url;
}

#[async_trait::async_trait]
pub trait Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>>;
	async fn recurse(&mut self, ignore: Gitignore) -> Result<SyncInfo>;
}

pub fn make(opts: &Opts, settings: &Settings) -> Result<Box<dyn Suite>> {
	let mut source = settings.source.clone();
	let default_username = if settings.source.scheme() == "ftp" {
		Some("anonymous")
	} else {
		None
	};
	if let Some(user) = settings.user.as_deref().or(default_username) {
		source.set_username(user).ok().context("Username into URL")?;
	}
	if let Some(pass) = settings.pass.as_deref() {
		source.set_password(Some(pass)).ok().context("Password into URL")?;
	}
	if let Some(super::Command::SyncData { user: Some(user), .. }) = &opts.cmd {
		source.set_username(user).ok().context("Username into URL")?;
	}
	if let Some(super::Command::SyncData { pass: Some(pass), .. }) = &opts.cmd {
		source.set_password(Some(pass)).ok().context("Password into URL")?;
	}
	match settings.source.scheme() {
		"ftp" => Ok(Box::new(crate::fromftp::Suite { source })),
		"file" => Ok(Box::new(crate::fromfile::Suite { source })),
		"http" | "https" => Ok(Box::new(crate::fromhttp::Suite::new(source))),
		"rsync" => Ok(Box::new(crate::fromrsync::Suite::new(source))),
		_ => anyhow::bail!("Invalid source url {}: only ftp is supported", settings.source),
	}
}

pub struct NullProvider {
	url: url::Url,
}
impl NullProvider {
	pub fn new() -> NullProvider {
		NullProvider {
			url: url::Url::parse("null:").unwrap(),
		}
	}
}
#[async_trait::async_trait]
impl Provider for NullProvider {
	async fn get(&self, p: &Path) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
		anyhow::bail!("Null provider asked to sync {:?}", p);
	}
	fn base(&self) -> &url::Url {
		&self.url
	}
}
