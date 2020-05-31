use crate::nabla::SyncActs;
use crate::mfs::Mfs;
use std::path::PathBuf;
use crate::provider::Provider;
use crate::nabla::SyncInfo;
use std::io::Cursor;
use anyhow::{ Result, Context, bail, ensure };
use std::time::SystemTime;

pub struct ToMfs {
	mfs: Mfs,
	base: PathBuf,
	id: String,
}

impl ToMfs {
	pub fn new(api: &str, base: PathBuf) -> Result<ToMfs> { Ok(ToMfs {
		mfs: Mfs::new(api)?,
		base: base,
		id: nanoid::nanoid!(),
	})}
	pub async fn prepare(&self) -> Result<()> {
		let curr = &self.base.join("curr");
		let currdata = &curr.join("data");
		let sync = &self.base.join("sync");
		let piddir = &sync.join("pid");
		if self.mfs.exists(sync).await? {
			if self.mfs.exists(piddir).await? {
				let list = self.mfs.ls(piddir).await?;
				if !list.is_empty() {
					bail!("pidfiles {:?} exists in {:?}", list, piddir)
				} else {
					self.mfs.rm_r(sync).await?;
					// TODO: recover
				}
			} else {
				self.mfs.rm_r(sync).await?;
				// The only reason I can imagine that this would happen is failure between
				// mkdirs and emplace of the lock in this function.
				// All other situations are eerie, so start afresh.
			}
		}
		self.mfs.mkdirs(sync).await?;
		if self.mfs.exists(currdata).await? {
			self.mfs.cp(currdata, &sync.join("data")).await?;
		}
		self.mfs.rm_r(&self.base.join("prev")).await.ok();
		// "Lock"
		let pid = format!("PID:{}@{}, {}\n",
			std::process::id(),
			hostname::get().map(|h| h.to_string_lossy().into_owned()).unwrap_or("unkown_host".to_owned()),
			SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Bogous clock?").as_secs(),
		);
		self.mfs.mkdirs(piddir)
			.await.context("Creating lock file directory")?;
		self.mfs.emplace(&piddir.join(&self.id), pid.len(), Cursor::new(pid))
			.await.context("Creating lock file")?;
		let locks = self.mfs.ls(piddir)
			.await.context("Check lock file")?;
		ensure!(locks.iter().map(|x| x.name.as_str() ).collect::<Vec<_>>() == vec![&self.id],
			"Locking race (Found {}), bailing out",
			locks.iter().map(|x| x.hash.as_str()).collect::<Vec<_>>().join(", "),
			// Mutually exclusive. Both may bail. Oh well.
		);
		Ok(())
	}
	pub async fn get_last_state(&self) -> Result<SyncInfo> {
		let meta = &self.base.join("curr").join("meta");
		match self.mfs.exists(meta).await? {
			true => {
				let bytes: Vec<u8> = self.mfs.read_fully(meta).await?;
				Ok(serde_json::from_slice(&bytes).context("JSON")?)
			},
			false => Ok(SyncInfo::new()),
		}
	}
	pub async fn apply(&self, sa: SyncActs, p: &dyn Provider) -> Result<()> {
		// TODO: desequentialize
		let sync = &self.base.join("sync");
		let syncdata = sync.join("data");

		let SyncActs { meta, get, delete } = sa;

		let metadata = serde_json::to_vec(&meta)?;
		self.mfs.emplace(&sync.join("meta"), metadata.len(), Cursor::new(metadata)).await?;

		for d in delete.iter() {
			self.mfs.rm_r(&syncdata.join(d)).await?;
		}
		for a in get.iter() {
			let pth = &syncdata.join(a);
			self.mfs.mkdirs(pth.parent().expect("Path to file should have a parent folder")).await?;
			self.mfs.emplace(pth, meta.files.get(a).map(|i| i.s).flatten().unwrap_or(0), p.get(a)).await?;
		}

		if delete.is_empty() && get.is_empty() {
			self.finalize_unchanged().await
				.context("No data synced, clean-up failed")
		} else {
			self.finalize_changes().await
				.context("Sync finished successfully, but could not be installed as current set")
		}
	}
	async fn finalize_changes(&self) -> Result<()> {
		let curr = &self.base.join("curr");
		let sync = &self.base.join("sync");
		let prev = &self.base.join("prev");
		let hascurr = self.mfs.exists(curr).await?;
		if hascurr {
			self.mfs.mv(curr, prev).await?;
		}
		self.mfs.cp(sync, curr).await?;
		self.mfs.rm_r(sync).await?;
		self.mfs.rm_r(&curr.join("pid")).await?;
		if hascurr {
			self.mfs.rm_r(prev).await?;
		}
		Ok(())
	}
	async fn finalize_unchanged(&self) -> Result<()> {
		let curr = &self.base.join("curr");
		let sync = &self.base.join("sync");
		if !self.mfs.exists(curr).await? {
			// WTF. Empty initial sync
			self.mfs.mkdir(&curr.join("data")).await?;
		} else {
			self.mfs.rm(&curr.join("meta")).await?;
		}
		self.mfs.cp(&sync.join("meta"), &curr.join("meta")).await?;
		self.mfs.rm_r(sync).await?;
		Ok(())
	}
	pub async fn failure_clean_lock(&self) -> Result<()> {
		self.mfs.rm_r(&self.base.join("sync").join("pid").join(&self.id)).await?;
		// Can't remove the dir, as there is no rmdir (that only removes empty dirs)
		// and rm -r might remove a lockfile that was just created
		Ok(())
	}
}
