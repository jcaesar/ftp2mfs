use anyhow::{ensure, Context, Result};
use chrono::prelude::*;
use clap::Clap;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use serde::Deserialize;
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
mod suite;
mod synlink;
mod tomfs;

use crate::nabla::SyncActs;
use crate::tomfs::ToMfs;

#[derive(Clap, Debug)]
#[clap(about, version)]
pub struct Opts {
	/// FTP username override
	#[clap(short, long)]
	user: Option<String>,
	/// FTP password override
	#[clap(short, long)]
	pass: Option<String>,
	/// IPFS files (mfs) base path
	#[clap(short, long)]
	config: PathBuf,
	/// IPFS api url
	#[clap(short, long, default_value = "http://localhost:5001/")]
	api: String,
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
	pass: Option<String>,
	/// workdir
	#[serde(default)]
	workdir: Option<PathBuf>,
	/// datadir
	target: PathBuf,
	/// Maximum depth up to which self-referential symlink (like x -> .) will be resolved
	#[serde(default)]
	max_symlink_cycle: u64,
}

#[tokio::main(basic_scheduler)]
async fn main() -> Result<()> {
	let start = Instant::now();

	env_logger::init();

	let opts: Opts = Opts::parse();

	let settings = get_settings(&opts.config).context(format!("Loading {:?}", &opts.config))?;

	let out = ToMfs::new(&opts.api, settings)
		.await
		.with_context(|| format!("Failed to access mfs on ipfs at {}", opts.api))?;
	match run_sync(&opts, &out).await {
		Ok(cid) => log::info!(
			"Finished sync in {} s, state: {}",
			(Instant::now() - start).as_secs(),
			cid
		),
		e @ Err(_) => {
			out.failure_clean_lock().await.ok();
			e?;
		}
	}

	Ok(())
}

async fn run_sync(opts: &Opts, out: &ToMfs) -> Result<String> {
	let settings = out.settings();
	let mut suite = suite::make(&opts, &settings)?;

	let current_set = out.prepare().await.with_context(|| {
		format!(
			"Failed to prepare mfs target folder in {:?} (and read current data state from {:?})",
			settings.workdir.as_ref().unwrap(),
			&settings.target
		)
	})?;

	let ignore = ignore(&settings.ignore).context("Constructing GlobSet for ignore list")?;

	let sync_start = Utc::now();

	let ups = suite.recurse(ignore).await.context("Retrieving file list")?;
	let sa =
		SyncActs::new(current_set, ups, settings.reprieve).context("Internal error: failed to generate delta")?;
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

pub(crate) fn ignore<V: AsRef<[T]>, T: AsRef<str>>(base: V) -> Result<Gitignore> {
	let mut globs = GitignoreBuilder::new("");
	for (i, ign) in base.as_ref().iter().map(AsRef::as_ref).enumerate() {
		globs
			.add_line(None, ign)
			.with_context(|| format!("Parse ignore index {}: {}", i, ign))?;
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
