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

	fn curr(&self)     -> PathBuf { self.base.join("curr") }
	fn sync(&self)     -> PathBuf { self.base.join("sync") }
	fn prev(&self)     -> PathBuf { self.base.join("prev") }
	fn currdata(&self) -> PathBuf { self.curr().join("data") }
	fn syncdata(&self) -> PathBuf { self.sync().join("data") }
	fn currmeta(&self) -> PathBuf { self.curr().join("meta") }
	fn syncmeta(&self) -> PathBuf { self.sync().join("meta") }
	fn currpid(&self)  -> PathBuf { self.curr().join("pid") }
	fn piddir(&self)   -> PathBuf { self.sync().join("pid") }

	pub async fn prepare(&self) -> Result<SyncInfo> {
		let recovery_required = if self.mfs.exists(self.sync()).await? {
			if self.mfs.exists(self.piddir()).await? {
				let list = self.mfs.ls(self.piddir()).await?;
				if !list.is_empty() {
					bail!("pidfiles {:?} exists in {:?}", list, self.piddir())
				} else {
					self.mfs.rm_r(self.sync()).await?;
					true
				}
			} else {
				self.mfs.rm_r(self.sync()).await?;
				// The only reason I can imagine that this would happen is failure between
				// mkdirs and emplace of the lock in this function.
				// All other situations are eerie, so start afresh.
				false
			}
		} else {
			false
		};
		self.mfs.mkdirs(self.sync()).await?;
		if self.mfs.exists(self.currdata()).await? {
			self.mfs.cp(self.currdata(), self.syncdata()).await?;
		}
		self.mfs.rm_r(self.prev()).await.ok();
		self.lock()
			.await.with_context(|| format!("Failed to create lock in {:?}", self.sync()))?;
		//Ok(())
		self.get_last_state().await // TODO!
	}
	async fn lock(&self) -> Result<()> {
		let pid = format!("PID:{}@{}, {}\n",
			std::process::id(),
			hostname::get().map(|h| h.to_string_lossy().into_owned()).unwrap_or("unkown_host".to_owned()),
			SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Bogous clock?").as_secs(),
		);
		self.mfs.mkdirs(self.piddir()).await?;
		self.mfs.emplace(self.piddir().join(&self.id), pid.len(), Cursor::new(pid)).await?;
		let locks = self.mfs.ls(self.piddir()).await?;
		ensure!(locks.iter().map(|x| x.name.as_str() ).collect::<Vec<_>>() == vec![&self.id],
			"Locking race (Found {}), bailing out",
			locks.iter().map(|x| x.hash.as_str()).collect::<Vec<_>>().join(", "),
			// Mutually exclusive. Both may bail. Oh well.
		);
		Ok(())
	}
	pub async fn get_last_state(&self) -> Result<SyncInfo> {
		match self.mfs.exists(self.currmeta()).await? {
			true => {
				let bytes: Vec<u8> = self.mfs.read_fully(self.currmeta()).await?;
				Ok(serde_json::from_slice(&bytes).context("JSON")?)
			},
			false => Ok(SyncInfo::new()),
		}
	}
	pub async fn apply(&self, sa: SyncActs, p: &dyn Provider) -> Result<()> {
		// TODO: desequentialize
		let SyncActs { meta, get, delete } = sa;

		let metadata = serde_json::to_vec(&meta)?;
		self.mfs.emplace(self.syncmeta(), metadata.len(), Cursor::new(metadata)).await?;

		for d in delete.iter() {
			self.mfs.rm_r(self.syncdata().join(d)).await?;
		}
		for a in get.iter() {
			let pth = self.syncdata().join(a);
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
		let hascurr = self.mfs.exists(self.curr()).await?;
		if hascurr {
			if self.mfs.exists(self.prev()).await? {
				self.mfs.rm_r(self.prev()).await?;
			}
			self.mfs.mv(self.curr(), self.prev()).await?;
		}
		self.mfs.cp(self.sync(), self.curr()).await?;
		self.mfs.rm_r(self.currpid()).await?;
		self.mfs.rm_r(self.sync()).await?;
		if hascurr {
			self.mfs.rm_r(self.prev()).await?;
		}
		Ok(())
	}
	async fn finalize_unchanged(&self) -> Result<()> {
		if !self.mfs.exists(self.curr()).await? {
			// WTF. Empty initial sync
			self.mfs.mkdir(self.currdata()).await?;
		} else {
			self.mfs.rm(self.currmeta()).await?;
		}
		self.mfs.cp(self.syncmeta(), self.currmeta()).await?;
		self.mfs.rm_r(self.sync()).await?;
		Ok(())
	}
	pub async fn failure_clean_lock(&self) -> Result<()> {
		self.mfs.rm_r(self.piddir().join(&self.id)).await?;
		// Can't remove the dir, as there is no rmdir (that only removes empty dirs)
		// and rm -r might remove a lockfile that was just created
		Ok(())
	}
}
