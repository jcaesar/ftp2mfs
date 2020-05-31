use clap::Clap;
use ftp::FtpStream;
use ftp::types::FileType;
use std::path::Path;
use anyhow::{ Result, Context };
use globset::{ Glob, GlobSetBuilder, GlobSet };

mod tomfs;
mod mfs;
mod provider;
mod fromftp;
mod nabla;

use crate::nabla::SyncActs;
use crate::fromftp::recursor::Recursor;
use crate::fromftp::provider::FtpProvider;
use crate::tomfs::ToMfs;
	
#[derive(Clap, Debug)]
struct Opts {
	/// Server to connect to
	#[clap(short, long)]
	connect: String,
	/// Change directory fater connecting
	#[clap(short = "d", long)]
	cwd: Option<String>,
	/// FTP username
	#[clap(short, long, default_value = "anonymous")]
	user: String,
	/// FTP password
	#[clap(short, long, default_value = "ftp2ipfs@liftm.de")]
	pass: String,
	//#[clap(short, long, parse(from_occurrences))]
	//verbose: i32,
	/// IPFS files (mfs) base path
	#[clap(short, long)]
	base: String,
	/// IPFS api url
	#[clap(short, long, default_value = "http://localhost:5001/")]
	api: String,
	/// Reprieve period for local files after deletion on server
	#[clap(short, long, default_value = "0 days")]
	reprieve: humantime::Duration,
	/// Ignore glob pattern when listing files on server
	#[clap(short, long)]
	ignore: Vec<String>,
}

#[tokio::main(basic_scheduler)]
async fn main() -> Result<()> {
	let opts: Opts = Opts::parse();

	let out = ToMfs::new(&opts.api, Path::new(&opts.base).to_path_buf())
		.with_context(|| format!("Failed to access mfs on ipfs at {}", opts.api))?;
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
	let current_set = out.prepare().await
		.with_context(|| format!("Failed to prepare mfs target folder in {} (and read current data state)", opts.base))?;

	let mut ftp_stream = ftp_connect(&opts)
		.context("Failed to establish FTP connection")?;
	for cwd in &opts.cwd {
		ftp_stream.cwd(cwd)
			.with_context(|| format!("Cannot switch to directory {}", cwd))?;
	}

	let ignore = globset(&opts.ignore)
		.context("Constructing GlobSet for ignore list")?;

	let ups = Recursor::run(&mut ftp_stream, &ignore)
		.context("Retrieving file list")?;
	let sa = SyncActs::new(current_set, ups, *opts.reprieve)
		.context("Internal error: failed to generate delta")?;

	println!("{:#?}", sa);
	out.apply(sa, &FtpProvider::new(ftp_stream)).await
		.context("Sync failure")?;

	Ok(())
}
	

fn ftp_connect(opts: &Opts) -> Result<FtpStream> {
	let mut ftp_stream = FtpStream::connect(format!("{}:21", opts.connect))?;
	ftp_stream.login(&opts.user, &opts.pass)?;
	ftp_stream.transfer_type(FileType::Binary)?;
	Ok(ftp_stream)
}

fn globset<T: AsRef<str>>(base: &[T]) -> Result<GlobSet> {
	let mut globs = GlobSetBuilder::new();
	for i in base {
		globs.add(Glob::new(i.as_ref())?);
	}
	return Ok(globs.build()?);
}
