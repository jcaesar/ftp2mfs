use crate::nabla::SyncActs;
use chrono::prelude::*;
use std::path::{ PathBuf, Path };
use crate::provider::Provider;
use crate::nabla::SyncInfo;
use futures::io::Cursor;
use anyhow::{ Result, Context, bail, ensure };
use std::time::SystemTime;
use std::collections::HashSet;
use crate::Settings;

pub struct ToMfs {
	mfs: mfs::Mfs,
	/// Attempt ID
	id: String,
	settings: Settings,
	settings_orig: Vec<u8>,
}

impl ToMfs {
	pub(crate) async fn new(api: &str, settings: (Settings, Vec<u8>)) -> Result<ToMfs> {
		let mfs = mfs::Mfs::new(api)?;
		let id = nanoid::nanoid!();
		let (settings, settings_orig) = settings;
		Ok(ToMfs { mfs, id, settings, settings_orig })
	}


	fn workdir(&self)  -> &Path   { &self.settings.workdir.as_ref().unwrap() }
	fn curr(&self)     -> &Path   { &self.settings.target }
	fn sync(&self)     -> PathBuf { self.workdir().join("sync") }
	fn prev(&self)     -> PathBuf { self.workdir().join("prev") }
	fn currdata(&self) -> PathBuf { self.curr().join("data") }
	fn syncdata(&self) -> PathBuf { self.sync().join("data") }
	fn currmeta(&self) -> PathBuf { self.curr().join("state") }
	fn syncmeta(&self) -> PathBuf { self.sync().join("state") }
	fn lastsync(&self) -> PathBuf { self.sync().join("lastsync") }
	fn currpid(&self)  -> PathBuf { self.curr().join("pid") }
	fn piddir(&self)   -> PathBuf { self.sync().join("pid") }
	fn lockf(&self)    -> PathBuf { self.piddir().join(&self.id) }
	pub(crate) fn settings(&self) -> &Settings { &self.settings }

	pub async fn prepare(&self) -> Result<SyncInfo> {
		self.mfs.rm_r(self.prev()).await.ok();
		let recovery_required = self.check_existing().await?;
		self.mfs.mkdirs(self.sync()).await?;
		if !recovery_required {
			if self.mfs.stat(self.currdata()).await?.is_some() {
				self.mfs.cp(self.currdata(), self.syncdata()).await?;
			}
		}
		self.lock()
			.await.with_context(|| format!("Failed to create lock in {:?}", self.sync()))?;
		self.mfs.put(self.sync().join("mirror"), Cursor::new(self.settings_orig.clone()))
			.await.context("Saving mirror settings in mfs")?;
		if recovery_required {
			self.recover()
				.await.context("failed to recover from failed sync")
		} else {
			self.get_state(self.currmeta())
				.await
		}
	}
	async fn check_existing(&self) -> Result<bool> {
		if self.mfs.stat(self.sync()).await?.is_some() {
			if self.mfs.stat(self.piddir()).await?.is_some() {
				let list = self.mfs.ls(self.piddir()).await?;
				if !list.is_empty() {
					bail!("pidfiles {:?} exists in {:?}", list, self.piddir()) // TODO: Error message
				} else {
					Ok(true)
				}
			} else {
				self.mfs.rm_r(self.sync()).await?;
				// The only reason I can imagine that this would happen is failure between
				// mkdirs and put of the lock in this function.
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
		self.mfs.put(self.lockf(), Cursor::new(pid)).await?;
		self.mfs.flush(self.piddir()).await?; // Probably unnecessary, but eh.
		let locks = self.mfs.ls(self.piddir()).await?;
		ensure!(locks == vec![Path::new(&self.id)],
			"Locking race (Found {}), bailing out",
			locks.iter().map(|l| l.to_string_lossy()).collect::<Vec<_>>().join(", "),
			// Mutually exclusive. Both may bail. Oh well.
		);
		Ok(())
	}
	async fn get_state(&self, p: PathBuf) -> Result<SyncInfo> {
		match self.mfs.stat(&p).await?.is_some() {
			true => {
				let bytes: Vec<u8> = self.mfs.get_fully(&p).await?;
				Ok(serde_json::from_slice(&bytes).context("JSON")?)
			},
			false => Ok(SyncInfo::new()),
		}
	}
	async fn recover(&self) -> Result<SyncInfo> {
		log::info!("Recovering from partial sync...");
		// Use the old metadata as a basis, but overwrite it with the new metadata where it can be
		// sure that the new metadata is accurate
		// (one catch: files of same size have to be assumed old)
		let mut curr = self.get_state(self.currmeta()).await?;
		let sync = self.get_state(self.syncmeta()).await?;
		let SyncActs { get, delete, .. } = SyncActs::new(curr.clone(), sync.clone(), std::time::Duration::from_secs(0))?;
		for d in delete.iter() {
			if !self.mfs.stat(self.syncdata().join(d)).await?.is_some() {
				curr.files.remove(d);
				// We could also restore it from curr, but we deleted it once because it was gone
				// from the server. Better keep it deleted locally, too.
			}
		}
		let mut paths_for_deletion: HashSet<&Path> = HashSet::new();
		let mut recovered_bytes: usize = 0;
		let mut recovered_files: usize = 0;
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
				(_, Some(newfile_size)) => {
					let stat = self.mfs.stat(anew).await?;
					if let Some(stat) = stat {
						match stat.size == newfile_size as u64 {
							true => AcceptSynced,
							false => ResetSyncToCurrent
						}
					} else {
						LeaveNonExisting
					}
				}
			};
			match (resolution, existing.is_some()) {
				(ResetSyncToCurrent, false) => {
					for a in a.ancestors() {
						paths_for_deletion.insert(a);
					}
				},
				(ResetSyncToCurrent, true) => {
					log::debug!("Recovery: resetting {:?}", anew);
                    self.mfs.rm_r(anew).await.ok();
					self.mfs.cp(self.currdata().join(&a), anew).await?
				},
				(AcceptSynced, _) => {
					log::debug!("Recovering {:?} -> {:?}", a, newfile);
					curr.files.insert(a.to_path_buf(), newfile.clone());
					recovered_bytes += newfile.s.unwrap_or(0);
					recovered_files += 1;
				},
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
				if self.mfs.stat(pfs).await?.is_some() {
					self.mfs.rm_r(pfs).await?;
				}
			}
		}
		log::info!("Recovered {} in {} files.", crate::bytes(recovered_bytes), recovered_files);
		Ok(curr)
	}
	pub async fn apply(&self, sa: SyncActs, p: &dyn Provider, sync_start: &DateTime<Utc>) -> Result<()> {
		let SyncActs { mut meta, mut get, mut delete } = sa;
		meta.cid = None;
		self.write_meta(&meta).await?;
        self.mfs.put(self.lastsync(), Cursor::new(sync_start.to_rfc3339().into_bytes())).await?;
		
		use futures_util::sink::SinkExt;
		use futures::stream::StreamExt;
		let (sender, mut receiver) = futures::channel::mpsc::channel::<anyhow::Result<()>>(1);
		let mut running: usize = 0;
		let max_running: usize = 5;

		while !delete.is_empty() || !get.is_empty() {
			if let Some(d) = delete.pop() {
				let mut sender = sender.clone();
				running += 1;
				let d_abs = self.syncdata().join(&d);
				let mfs = self.mfs.clone();
				tokio::spawn(async move {
					let res = mfs.rm_r(d_abs).await
						.context(format!("Deletion: {:?}", d));
					sender.send(res).await.expect("Parallelism synchronization");
				});
			}
			if let Some(a) = get.pop() {
				let mut _sender = sender.clone();
				running += 1;
				let pth = self.syncdata().join(&a);
				let stream: Box<dyn futures::AsyncRead + Send + Sync + Unpin> = p.log_and_get(&a);
				let mfs = self.mfs.clone();
				tokio::spawn(async move {
					let res = mfs.put(pth, stream)
						//.await.context(format!("Requisiton: {:?}", &a))
					;
					//_sender.send(res).await.expect("Parallelism synchronization");
				});
			}
			if running > max_running - 2 {
				receiver.next().await.expect("Paralleism synchronization")?;
			}
		}
		std::mem::drop(sender);
		while let Some(res) = receiver.next().await {
			res?
		}

		meta.cid = Some(self.mfs.stat(self.syncdata()).await?.context(format!("File {:?} vanished", self.syncdata()))?.hash);
		self.write_meta(&meta).await?;

		self.finalize()
			.await.context("Sync finished successfully, but could not be installed as current set")?;
		self.mfs.flush(self.curr()).await?;
		self.mfs.flush(self.workdir().parent().expect("If workdir and target have a disjoint suffix, they must have parents")).await?;
		Ok(())
	}
	async fn finalize(&self) -> Result<()> {
		if self.mfs.stat(self.curr()).await?.is_some() {
			self.mfs.mv(self.curr(), self.prev()).await?
		}
		self.mfs.mv(self.sync(), self.curr()).await?;
		self.mfs.rm_r(self.workdir()).await?;
		self.mfs.rm_r(self.currpid()).await?;
		Ok(())
	}
	pub async fn failure_clean_lock(&self) -> Result<()> {

		self.mfs.rm_r(self.currpid()).await.ok();
		self.mfs.rm(self.lockf()).await?;
		// Can't remove the sync pid dir, as there is no rmdir (that only removes empty dirs)
		// and rm -r might remove a lockfile that was just created
		Ok(())
	}

	pub async fn write_meta(&self, meta: &SyncInfo) -> Result<()> {
		let metadata = serde_json::to_vec(&meta)?;
		self.mfs.put(self.syncmeta(), Cursor::new(metadata)).await?;
		Ok(())
	}
}
