use clap::Clap;
use ftp::FtpStream;
use ftp::types::FileType;
use std::path::Path;
use anyhow::{ Result, Context };

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
}

#[tokio::main(basic_scheduler)]
async fn main() -> Result<()> {
	let opts: Opts = Opts::parse();

	let out = ToMfs::new(&opts.api, Path::new(&opts.base).to_path_buf())
		.with_context(|| format!("Failed to access mfs on ipfs at {}", opts.api))?;
	out.prepare().await
		.with_context(|| format!("Failed to prepare mfs target folder {}", opts.base))?;
	let cur = out.get_last_state().await
		//.context("Failed to retrieve state of existing files")
		.context("TODO: Allow recovering it")?;

	let mut ftp_stream = ftp_connect(&opts)
		.context("Failed to establish FTP connection")?;

	for ref cwd in opts.cwd {
		ftp_stream.cwd(cwd)
			.with_context(|| format!("Cannot switch to directory {}", cwd))?;
	}

	let ups = Recursor::run(&mut ftp_stream)
		.context("Retrieving file list")?;
	let sa = SyncActs::new(cur, ups, *opts.reprieve)
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
