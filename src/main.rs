use clap::Clap;
use ftp::FtpStream;
use ftp::types::FileType;
use std::path::Path;

mod tomfs;
mod mfs;
mod provider;
mod fromftp;
mod nabla;

use crate::nabla::SyncActs;
use crate::fromftp::recursor::Recursor;
use crate::fromftp::provider::FtpProvider;
use crate::tomfs::ToMfs;

#[tokio::main(basic_scheduler)]
async fn main() {
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
	let opts: Opts = Opts::parse();

	let out = ToMfs::new(&opts.api, Path::new(&opts.base).to_path_buf());
	out.prepare().await;
	let cur = out.get_last_state().await;

	let mut ftp_stream = FtpStream::connect(format!("{}:21", opts.connect)).unwrap();
	ftp_stream.login(&opts.user, &opts.pass).unwrap();
	ftp_stream.transfer_type(FileType::Binary).unwrap();

	for ref cwd in opts.cwd {
		ftp_stream.cwd(cwd).unwrap();
	}

	let ups = Recursor::run(&mut ftp_stream);
	let sa = SyncActs::new(cur, ups, *opts.reprieve);

	println!("{:#?}", sa);
	out.apply(sa, &FtpProvider::new(ftp_stream)).await;
}
