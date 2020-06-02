use clap::Clap;
use ftp::FtpStream;
use ftp::types::FileType;
use std::path::Path;
use anyhow::{ Result, Context, ensure };
use ignore::gitignore::{ GitignoreBuilder, Gitignore };
use url::Url;
use serde::Deserialize;
use std::time::Duration;

mod tomfs;
mod mfs;
mod provider;
mod fromftp;
mod nabla;
#[cfg(test)]
mod globtest;

use crate::nabla::SyncActs;
use crate::fromftp::recursor::Recursor;
use crate::fromftp::provider::FtpProvider;
use crate::tomfs::ToMfs;
	
#[derive(Clap, Debug)]
struct Opts {
	/// FTP username override
	#[clap(short, long)]
	user: Option<String>,
	/// FTP password override
	#[clap(short, long)]
	pass: Option<String>,
	/// IPFS files (mfs) base path
	#[clap(short, long)]
	base: String,
	/// IPFS api url
	#[clap(short, long, default_value = "http://localhost:5001/")]
	api: String,
}

#[derive(Deserialize, Debug)]
struct Settings {
	#[serde(with = "url_serde")]
	source: Url,
	/// Reprieve period for local files after deletion on server
	#[serde(with = "serde_humantime", default)]
	reprieve: Duration,
	/// Ignore glob patterns when listing files on server (gitignore style)
	ignore: Vec<String>,
	/// FTP username - mostly just "anonymous"
	#[serde(default = "const_anonymous")]
	user: String,
	/// FTP password - mostly, just an e-mail
	pass: Option<String>,
}

fn const_anonymous() -> String { "anonymous".to_owned() }

#[tokio::main(basic_scheduler)]
async fn main() -> Result<()> {
	let opts: Opts = Opts::parse();

	let out = ToMfs::new(&opts.api, Path::new(&opts.base).to_path_buf())
		.await.with_context(|| format!("Failed to access mfs on ipfs at {}", opts.api))?;
	match run_sync(&opts, &out).await {
		Ok(_) => (),
		e@Err(_) => {
			out.failure_clean_lock().await.ok();
			e?;
		}
	}

	Ok(())
}

async fn run_sync(opts: &Opts, out: &ToMfs) -> Result<()> {
	let settings = out.settings();
	ensure!(settings.source.scheme() == "ftp",
		"Invalid source url {}: only ftp is supported (Will I support others? Who knows!)", settings.source);

	let current_set = out.prepare().await
		.with_context(|| format!("Failed to prepare mfs target folder in {} (and read current data state)", opts.base))?;

	let mut ftp_stream = ftp_connect(&opts, &settings)
		.context("Failed to establish FTP connection")?;

	let ignore = ignore(&settings.ignore)
		.context("Constructing GlobSet for ignore list")?;

	let ups = Recursor::run(&mut ftp_stream, &ignore)
		.context("Retrieving file list")?;
	let sa = SyncActs::new(current_set, ups, settings.reprieve)
		.context("Internal error: failed to generate delta")?;

	println!("{:#?}", sa);
	out.apply(sa, &FtpProvider::new(ftp_stream)).await
		.context("Sync failure")?;

	Ok(())
}
	

fn ftp_connect(opts: &Opts, settings: &Settings) -> Result<FtpStream> {
	let host = settings.source.host_str()
		.context(format!("No host specified in source url {}", settings.source))?;
	let port = settings.source.port().unwrap_or(21);
	let mut ftp_stream = FtpStream::connect(format!("{}:{}", host, port))?;
	let user = opts.user.as_ref().unwrap_or(&settings.user);
	let pass = opts.pass.as_ref().or(settings.pass.as_ref())
		.context("No FTP password specified")?;
	ftp_stream.login(&user, &pass)?;
	ftp_stream.transfer_type(FileType::Binary)?;
	ftp_stream.cwd(settings.source.path())
		.with_context(|| format!("Cannot switch to directory {}", settings.source.path()))?;
	Ok(ftp_stream)
}

pub(crate) fn ignore<V: AsRef<[T]>, T: AsRef<str>>(base: V) -> Result<Gitignore> {
	let mut globs = GitignoreBuilder::new("");
	for (i, ign) in base.as_ref().iter().map(AsRef::as_ref).enumerate() {
		globs.add_line(None, ign)
			.with_context(|| format!("Parse ignore index {}: {}", i, ign))?;
	}
	return Ok(globs.build()?);
}
