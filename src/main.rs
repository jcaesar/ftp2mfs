use clap::Clap;
use chrono::prelude::*;
use std::path::{ Path, PathBuf };
use anyhow::{ Result, Context, ensure };
use ignore::gitignore::{ GitignoreBuilder, Gitignore };
use url::Url;
use serde::Deserialize;
use std::time::Duration;
pub fn bytes(b: usize) -> bytesize::ByteSize {
	use std::convert::TryInto;
	bytesize::ByteSize::b(b.try_into().expect("outlandish byte count"))
}

mod tomfs;
mod fromftp;
mod fromfile;
mod fromhttp;
mod fromrsync;
mod nabla;
#[cfg(test)]
mod globtest;
mod suite;
mod synlink;

use crate::nabla::SyncActs;
use crate::tomfs::ToMfs;
	
#[derive(Clap, Debug)]
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
}

#[tokio::main(basic_scheduler)]
async fn main() -> Result<()> {
	env_logger::init();

	let opts: Opts = Opts::parse();

	let settings = get_settings(&opts.config)
		.context(format!("Loading {:?}", &opts.config))?;

	let out = ToMfs::new(&opts.api, settings)
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
	let mut suite = suite::make(&opts, &settings)?;

	let current_set = out.prepare().await
		.with_context(|| format!("Failed to prepare mfs target folder in {:?} (and read current data state from {:?})", settings.workdir.as_ref().unwrap(), &settings.target))?;

	let ignore = ignore(&settings.ignore)
		.context("Constructing GlobSet for ignore list")?;

	let sync_start = Utc::now();

	let ups = suite.recurse(ignore)
		.await.context("Retrieving file list")?;
	let sa = SyncActs::new(current_set, ups, settings.reprieve)
		.context("Internal error: failed to generate delta")?;
	let stats = sa.stats();
	let deletestats = match sa.delete.is_empty() {
		true => "".to_owned(),
		false => format!(" and delete {} files", sa.delete.len()),
	};
	let reprievestats = match stats.reprieve_files {
		0 => "".to_owned(),
		_ => format!(", {} in {} files already deleted on server",
			bytes(stats.reprieve_bytes), stats.reprieve_files
		),
	};
	log::info!("Will sync at least {} in {} files{}, final size: {} in {} files{}.",
		bytes(stats.get_bytes), sa.get.len(),
		deletestats,
		bytes(stats.final_bytes), sa.meta.files.len(),
		reprievestats,
	);

	out.apply(sa, &*suite.provider().await?, &sync_start).await
		.context("Sync failure")?;

	Ok(())
}
	
pub(crate) fn ignore<V: AsRef<[T]>, T: AsRef<str>>(base: V) -> Result<Gitignore> {
	let mut globs = GitignoreBuilder::new("");
	for (i, ign) in base.as_ref().iter().map(AsRef::as_ref).enumerate() {
		globs.add_line(None, ign)
			.with_context(|| format!("Parse ignore index {}: {}", i, ign))?;
	}
	return Ok(globs.build()?);
}

fn get_settings(path: &Path) -> Result<(Settings, Vec<u8>)> {
	let bytes: Vec<u8> = std::fs::read(path)?;
	let mut struc: Settings = serde_yaml::from_slice(&bytes)
		.context("Could not parse YAML")?;
	if struc.workdir.is_none() {
		let hash = blake3::Hasher::new().update(&bytes).finalize().to_hex();
		struc.workdir = Some(Path::new("/temp").join(hash.to_string()));
		log::warn!("workdir not specified in settings, defaulting to {:?}", struc.workdir.as_ref().unwrap());
	}
	ensure!(struc.target.is_absolute(), "target path {:?} is not absolute", &struc.target);
	let workdir = struc.workdir.as_ref().unwrap();
	ensure!(workdir.is_absolute(), "work path {:?} is not absolute", &struc.workdir);
	ensure!(!(workdir.starts_with(&struc.target) || struc.target.starts_with(workdir)),
		"work path {:?} and target path path {:?} cannot contain one another", &workdir, &struc.target);
	return Ok((struc, bytes));
}
