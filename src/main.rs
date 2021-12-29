use anyhow::{ensure, Context, Result};
use chrono::prelude::*;
use custom_debug::Debug;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use serde::Deserialize;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::Instant;
use url::Url;
pub fn bytes(b: usize) -> bytesize::ByteSize {
	use std::convert::TryInto;
	bytesize::ByteSize::b(b.try_into().expect("outlandish byte count"))
}

mod fromfile;
mod fromftp;
mod fromhttp;
mod fromrsync;
#[cfg(test)]
mod globtest;
mod nabla;
mod semaphored;
mod suite;
mod synlink;
mod tomfs;

use crate::nabla::SyncActs;
use crate::tomfs::ToMfs;

fn snip_fmt<T>(val: &Option<T>, f: &mut fmt::Formatter) -> fmt::Result {
	write!(
		f,
		"{}",
		match val {
			Some(_) => "Some(...snip)",
			None => "None",
		}
	)
}

#[derive(clap::Parser, Debug)]
#[clap(about, version, author)]
pub struct Opts {
	/// IPFS files (mfs) base path
	#[clap(short, long)]
	config: PathBuf,
	/// IPFS api url
	#[clap(short, long, default_value = "http://localhost:5001/", env = "IPFS_API")]
	api: String,
	/// Verbosity
	///
	/// Takes an env_logger spec when running normally (e.g. info,arrsync=debug,ftp2mfs=trace)
	/// Takes a single log level name when running with systemd-journald (e.g. warn)
	#[clap(short, long, default_value = "info", env = "RUST_LOG")]
	log_level: String,
	#[clap(subcommand)]
	cmd: Option<Command>,
}

#[derive(clap::Subcommand, Debug)]
pub enum Command {
	/// Main operation - Sync data from source to MFS
	#[clap(name = "sync")]
	SyncData {
		/// FTP username override
		#[clap(short, long)]
		user: Option<String>,
		/// FTP password override
		#[clap(short, long)]
		#[debug(with = "snip_fmt")]
		pass: Option<String>,
	},
	/// Verify (and rewrite if necessary) the state file
	///
	/// The state file holds a list of files, their sizes and modification dates
	/// If something else modifies the data folder
	Scrub(Scrub),
}

#[derive(clap::Parser, Debug)]
pub struct Scrub {
	/// Whether to pretend that files existing in the folder structure but not in the
	/// state file (thus likely also not on the server) have existed on the server
	/// until the verify operation.
	///
	/// If unset, they'll be deleted on the next sync operation, otherwise only after the
	/// reprieve period.
	#[clap(short, long)]
	allow_reprieve: bool,
}

#[derive(Deserialize, Debug)]
pub struct Settings {
	source: Url,
	/// Reprieve period for local files after deletion on server
	#[serde(with = "serde_humantime", default)]
	reprieve: Duration,
	/// Ignore glob patterns when listing files on server (gitignore style)
	#[serde(default)]
	ignore: Vec<String>,
	/// username - Defaults to "anonymous" for FTP
	#[serde(default)]
	user: Option<String>,
	/// password - for FTP mostly just an e-mail
	#[serde(default)]
	#[debug(with = "snip_fmt")]
	pass: Option<String>,
	/// workdir
	#[serde(default)]
	workdir: Option<PathBuf>,
	/// datadir
	target: PathBuf,
	/// Maximum depth up to which self-referential symlink (like x -> .) will be resolved
	#[serde(default)]
	max_symlink_cycle: u64,
	/// List of patterns for files that are never modified
	///
	/// Will also skip the metadata retrieval on sources where that is expensive (HTTP, FTP)
	#[serde(default)]
	solid: Vec<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
	let start = Instant::now();
	let mut opts: Opts = clap::Parser::parse();
	if opts.cmd.is_none() {
		opts.cmd = Some(Command::SyncData { user: None, pass: None });
	}

	if systemd_journal_logger::connected_to_journal() {
		systemd_journal_logger::init_with_extra_fields(vec![("VERSION", env!("CARGO_PKG_VERSION"))])
			.context("Failed to init journald logger")?;
		log::set_max_level(
			opts.log_level
				.parse()
				.context(format!("Can't parse {} as a log level", opts.log_level))?,
		);
	} else {
		env_logger::Builder::from_default_env()
			.parse_filters(&opts.log_level)
			.init();
	}

	log::debug!("Command Line {:#?}", opts);

	let settings = get_settings(&opts.config).context(format!("Loading {:?}", &opts.config))?;
	log::debug!("Loaded {:#?}", settings);

	let out = ToMfs::new(&opts.api, settings)
		.await
		.with_context(|| format!("Failed to access mfs on ipfs at {}", opts.api))?;
	match opts.cmd.as_ref().unwrap() {
		Command::SyncData { .. } => match run_sync(&opts, &out).await.context("sync") {
			Ok(cid) => log::info!(
				"Finished sync in {} s, state: {}",
				(Instant::now() - start).as_secs(),
				cid
			),
			e @ Err(_) => {
				out.failure_clean_lock().await.ok();
				e?;
			}
		},
		Command::Scrub(vs) => {
			anyhow::ensure!(
				!out.check_existing().await?,
				"Working dir {:?} exists - can't start (or resume) verify-state",
				out.workdir()
			);
			match run_check(&vs, &out).await.context("verify-state") {
				Ok(()) => log::info!("Finished check-state in {} s.", (Instant::now() - start).as_secs()),
				e @ Err(_) => {
					// Can't resume a failed meta verification
					out.failure_clean_all().await.ok();
					e?;
				}
			}
		}
	}

	Ok(())
}

async fn run_sync(opts: &Opts, out: &ToMfs) -> Result<String> {
	let settings = out.settings();
	let mut suite = suite::make(&opts, &settings)?;

	let current_set = out.prepare().await?;

	let ignore = globlist(&settings.ignore).context("Constructing GlobSet for ignore list")?;
	let solid = globlist(&settings.solid).context("Constructing GlobSet for solid file list")?;

	let sync_start = Utc::now();

	let ups = suite.recurse(ignore, solid).await.context("Retrieving file list")?;
	let sa = SyncActs::new(current_set, ups, &settings).context("Internal error: failed to generate delta")?;
	let stats = sa.stats();
	let deletestats = match sa.delete.is_empty() {
		true => "".to_owned(),
		false => format!(" and delete {} files", sa.delete.len()),
	};
	let reprievestats = match stats.reprieve_files {
		0 => "".to_owned(),
		_ => format!(
			", {} in {} files already deleted on server",
			bytes(stats.reprieve_bytes),
			stats.reprieve_files
		),
	};
	log::info!(
		"Will sync at least {} in {} files{}, final size: {} in {} files{}.",
		bytes(stats.get_bytes),
		sa.get.len(),
		deletestats,
		bytes(stats.final_bytes),
		sa.meta.files.len(),
		reprievestats,
	);

	let cid = out
		.apply(sa, &*suite.provider().await?, &sync_start)
		.await
		.context("Sync failure")?;

	Ok(cid)
}

async fn run_check(opts: &Scrub, out: &ToMfs) -> Result<()> {
	let fake_deleted = if opts.allow_reprieve { Some(Utc::now()) } else { None };

	let current_set = out.prepare().await?;

	let fix = out
		.check_state(current_set, fake_deleted)
		.await
		.context("Recursively inspect existing files")?;
	out.apply(fix, &suite::NullProvider::new(), &Utc::now())
		.await
		.context("Fixing data folder")?;

	Ok(())
}

pub(crate) fn globlist<V: AsRef<[T]>, T: AsRef<str>>(base: V) -> Result<Gitignore> {
	let mut globs = GitignoreBuilder::new("");
	for (i, ign) in base.as_ref().iter().map(AsRef::as_ref).enumerate() {
		globs
			.add_line(None, ign)
			.with_context(|| format!("Parse glob list entry {}: {}", i, ign))?;
	}
	return Ok(globs.build()?);
}

fn get_settings(path: &Path) -> Result<(Settings, Vec<u8>)> {
	let bytes: Vec<u8> = std::fs::read(path)?;
	let mut struc: Settings = serde_yaml::from_slice(&bytes).context("Could not parse YAML")?;
	if struc.workdir.is_none() {
		let hash = blake3::Hasher::new().update(&bytes).finalize().to_hex();
		struc.workdir = Some(Path::new("/temp").join(hash.to_string()));
		log::warn!(
			"workdir not specified in settings, defaulting to {:?}",
			struc.workdir.as_ref().unwrap()
		);
	}
	ensure!(
		struc.target.is_absolute(),
		"target path {:?} is not absolute",
		&struc.target
	);
	let workdir = struc.workdir.as_ref().unwrap();
	ensure!(workdir.is_absolute(), "work path {:?} is not absolute", &struc.workdir);
	ensure!(
		!(workdir.starts_with(&struc.target) || struc.target.starts_with(workdir)),
		"work path {:?} and target path path {:?} cannot contain one another",
		&workdir,
		&struc.target
	);
	return Ok((struc, bytes));
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
	env_logger::builder()
		.is_test(true)
		.filter(None, log::LevelFilter::Debug)
		.filter(Some("libunftp"), log::LevelFilter::Warn)
		.try_init()
		.unwrap();
}
