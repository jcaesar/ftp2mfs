#[cfg(test)]
mod memftp;
mod provider;
mod recursor;

use crate::nabla::SyncInfo;
use crate::suite::*;
use anyhow::{Context, Result};
use async_ftp::types::FileType;
use async_ftp::FtpStream;
use ignore::gitignore::Gitignore;

pub struct Suite {
	pub source: url::Url,
}

impl Suite {
	async fn ftp_connect(&self) -> Result<FtpStream> {
		let host = self
			.source
			.host_str()
			.context(format!("No host specified in source url {}", self.source))?;
		let port = self.source.port().unwrap_or(21);
		let mut ftp_stream = FtpStream::connect(format!("{}:{}", host, port)).await?;
		let user = self.source.username();
		let user = if user == "" { "anonymous" } else { user };
		let pass = self
			.source
			.password()
			.context("Password must be set for FTP - many public servers you to provide your e-mail address")?;
		ftp_stream.login(user, pass).await?;
		ftp_stream.transfer_type(FileType::Binary).await?;
		ftp_stream
			.cwd(self.source.path())
			.await
			.with_context(|| format!("Cannot switch to directory {}", self.source.path()))?;
		Ok(ftp_stream)
	}
}

#[async_trait::async_trait]
impl crate::suite::Suite for Suite {
	async fn provider(&self) -> Result<Box<dyn Provider>> {
		let conn = self.ftp_connect().await.context("FTP connect for data retrieval")?;
		Ok(Box::new(provider::FtpProvider::new(conn, self.source.clone())))
	}
	async fn recurse(&mut self, ignore: Gitignore) -> Result<SyncInfo> {
		let mut conn = self.ftp_connect().await.context("FTP connect for data list")?;
		Ok(recursor::Recursor::run(&mut conn, &ignore).await?)
	}
}
