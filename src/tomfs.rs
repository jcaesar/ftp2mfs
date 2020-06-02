use crate::nabla::SyncActs;
use crate::mfs::Mfs;
use std::path::{ PathBuf, Path };
use crate::provider::Provider;
use crate::nabla::SyncInfo;
use std::io::Cursor;
use anyhow::{ Result, Context, bail, ensure };
use std::time::SystemTime;
use std::collections::HashSet;

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
	fn lockf(&self)    -> PathBuf { self.piddir().join(&self.id) }

	pub async fn prepare(&self) -> Result<SyncInfo> {
		self.mfs.rm_r(self.prev()).await.ok();
		let recovery_required = self.check_existing().await?;
		self.mfs.mkdirs(self.sync()).await?;
		if !recovery_required {
			if self.mfs.exists(self.currdata()).await? {
				self.mfs.cp(self.currdata(), self.syncdata()).await?;
			}
		}
		self.lock()
			.await.with_context(|| format!("Failed to create lock in {:?}", self.sync()))?;
		if recovery_required {
			self.recover()
				.await.context("failed to recover from failed sync")
		} else {
			self.get_state(self.currmeta())
				.await
		}
	}
	async fn check_existing(&self) -> Result<bool> {
		if self.mfs.exists(self.sync()).await? {
			if self.mfs.exists(self.piddir()).await? {
				let list = self.mfs.ls(self.piddir()).await?;
				if !list.is_empty() {
					bail!("pidfiles {:?} exists in {:?}", list, self.piddir()) // TODO: Error message
				} else {
					Ok(true)
				}
			} else {
				self.mfs.rm_r(self.sync()).await?;
				// The only reason I can imagine that this would happen is failure between
				// mkdirs and emplace of the lock in this function.
				// All other situations are eerie, so start afresh.
				Ok(false)
			}
		} else {
			Ok(false)
		}
	}
	async fn lock(&self) -> Result<()> {
		let pid = format!("PID:{}@{}, {}\n",
			std::process::id(),
			hostname::get().map(|h| h.to_string_lossy().into_owned()).unwrap_or("unkown_host".to_owned()),
			SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("Bogous clock?").as_secs(),
		);
		self.mfs.mkdirs(self.piddir()).await?;
		self.mfs.emplace(self.lockf(), pid.len(), Cursor::new(pid)).await?;
		self.mfs.flush(self.piddir()).await?; // Probably unnecessary, but eh.
		let locks = self.mfs.ls(self.piddir()).await?;
		ensure!(locks.iter().map(|x| x.name.as_str() ).collect::<Vec<_>>() == vec![&self.id],
			"Locking race (Found {}), bailing out",
			locks.iter().map(|x| x.hash.as_str()).collect::<Vec<_>>().join(", "),
			// Mutually exclusive. Both may bail. Oh well.
		);
		Ok(())
	}
	async fn get_state(&self, p: PathBuf) -> Result<SyncInfo> {
		match self.mfs.exists(&p).await? {
			true => {
				let bytes: Vec<u8> = self.mfs.read_fully(&p).await?;
				Ok(serde_json::from_slice(&bytes).context("JSON")?)
			},
			false => Ok(SyncInfo::new()),
		}
	}
	async fn recover(&self) -> Result<SyncInfo> {
		// Use the old metadata as a basis, but overwrite it with the new metadata where it can be
		// sure that the new metadata is accurate
		// (one catch: files of same size have to be assumed old)
		let mut curr = self.get_state(self.currmeta()).await?;
		let sync = self.get_state(self.syncmeta()).await?;
		let SyncActs { get, delete, .. } = SyncActs::new(curr.clone(), sync.clone(), std::time::Duration::from_secs(0))?;
		for d in delete.iter() {
			if !self.mfs.exists(self.syncdata().join(d)).await? {
				curr.files.remove(d);
				// We could also restore it from curr, but we deleted it once because it was gone
				// from the server. Better keep it deleted locally, too.
			}
		}
		let mut paths_for_deletion: HashSet<&Path> = HashSet::new();
		for a in get.iter() {
			let anew = &self.syncdata().join(&a);
			enum State { ResetSyncToCurrent, AcceptSynced, LeaveNonExisting };
			use State::*;
			use crate::nabla::FileInfo;
			let existing = curr.files.get(a);
			let newfile = sync.files.get(a).unwrap();
			let resolution = match (existing, newfile.s) {
				(_, None) => ResetSyncToCurrent,
				(Some(FileInfo { s: None, .. }), _) => ResetSyncToCurrent,
				(Some(FileInfo { s: Some(currfile_size), .. }), Some(ref newfile_size))
					if currfile_size == newfile_size => ResetSyncToCurrent, // (the catch)
				(_, Some(newfile_size)) =>
					if self.mfs.exists(anew).await? { // TODO: don't stat twice
						let stat = self.mfs.stat(anew).await?;
						match stat.size == newfile_size as u64 {
							true => AcceptSynced,
							false => ResetSyncToCurrent
						}
					} else {
						LeaveNonExisting
					}
			};
			match (resolution, existing.is_some()) {
				(ResetSyncToCurrent, false) => {
					for a in a.ancestors() {
						paths_for_deletion.insert(a);
					}
				},
				(ResetSyncToCurrent, true) => { self.mfs.cp(self.currdata().join(&a), anew).await? },
				(AcceptSynced, _) => { curr.files.insert(a.to_path_buf(), newfile.clone()); },
				(LeaveNonExisting, _) => ()
			};
		}
		let curr = curr;
		for (f, _) in curr.files.iter() {
			for a in f.ancestors() {
				paths_for_deletion.remove(a);
			}
		}
		for p in paths_for_deletion.iter() {
			if p.parent().map(|p| !paths_for_deletion.contains(p)).unwrap_or(false) {
				let pfs = &self.syncdata().join(p);
				if self.mfs.exists(pfs).await? {
					self.mfs.rm_r(pfs).await?;
				}
			}
		}
		Ok(curr)
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
			self.finalize_unchanged()
				.await.context("No data synced, clean-up failed")?
		} else {
			self.finalize_changes()
				.await.context("Sync finished successfully, but could not be installed as current set")?
		}
		self.mfs.flush(&self.base).await?;
		Ok(())
	}
	async fn finalize_changes(&self) -> Result<()> {
		let hascurr = self.mfs.exists(self.curr()).await?;
		if hascurr {
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
		self.mfs.rm_r(self.currpid()).await.ok();
		self.mfs.rm(self.lockf()).await?;
		// Can't remove the sync pid dir, as there is no rmdir (that only removes empty dirs)
		// and rm -r might remove a lockfile that was just created
		Ok(())
	}
}
