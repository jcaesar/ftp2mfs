use crate::nabla::SyncActs;
use crate::mfs::Mfs;
use std::path::PathBuf;
use crate::provider::Provider;
use crate::nabla::SyncInfo;
use std::io::Cursor;

pub struct ToMfs {
	mfs: Mfs,
	base: PathBuf,
}

impl ToMfs {
	pub fn new(api: &str, base: PathBuf) -> ToMfs { ToMfs {
		mfs: Mfs::new(api),
		base: base,
	}}
	pub async fn prepare(&self) {
		let curr = &self.base.join("curr");
		let currdata = &curr.join("data");
		let sync = &self.base.join("sync");
		let pidfile = &sync.join("pid");
		if self.mfs.stat(sync).await.is_ok() {
			if self.mfs.stat(pidfile).await.is_ok() {
				panic!("Pidfile {:?} exists", pidfile)
			} else {
				self.mfs.rm_r(sync).await.unwrap();
			}
		}
		self.mfs.mkdirs(sync).await.unwrap();
		if self.mfs.stat(currdata).await.is_ok() {
			self.mfs.cp(currdata, &sync.join("data")).await.unwrap();
		}
		self.mfs.rm_r(&self.base.join("prev")).await.ok();
		// "Lock"
		let pid = Cursor::new(format!("{}", std::process::id()));
		self.mfs.emplace(pidfile, 0, pid).await.unwrap();
	}
	pub async fn get_last_state(&self) -> SyncInfo {
		let meta = &self.base.join("curr").join("meta");
		match self.mfs.stat(meta).await {
			Ok(_) => serde_json::from_slice(&self.mfs.read_fully(meta).await.unwrap()).unwrap(),
			Err(_) => SyncInfo::new(),
		}
	}
	pub async fn apply(&self, sa: SyncActs, p: &dyn Provider) {
		// TODO: desequentialize
		let sync = &self.base.join("sync");
		let syncdata = sync.join("data");

		let SyncActs { meta, get, delete } = sa;

		let metadata = serde_json::to_vec(&meta).unwrap();
		self.mfs.emplace(&sync.join("meta"), metadata.len(), Cursor::new(metadata)).await.unwrap();

		for d in delete.iter() {
			self.mfs.rm_r(&syncdata.join(d)).await.unwrap();
		}
		for a in get.iter() {
			let pth = &syncdata.join(a);
			self.mfs.mkdirs(pth.parent().unwrap()).await.unwrap();
			self.mfs.emplace(pth, meta.files.get(a).map(|i| i.s).flatten().unwrap_or(0), p.get(a)).await.unwrap();
		}

		if delete.is_empty() && get.is_empty() {
			self.finalize_unchanged().await
		} else {
			self.finalize_changes().await
		}
	}
	async fn finalize_changes(&self) {
		let curr = &self.base.join("curr");
		let sync = &self.base.join("sync");
		let prev = &self.base.join("prev");
		let hascurr = self.mfs.stat(curr).await.is_ok();
		if hascurr {
			self.mfs.mv(curr, prev).await.unwrap();
		}
		self.mfs.cp(sync, curr).await.unwrap();
		self.mfs.rm_r(sync).await.unwrap();
		self.mfs.rm_r(&curr.join("pid")).await.unwrap();
		if hascurr {
			self.mfs.rm_r(prev).await.unwrap();
		}
	}
	async fn finalize_unchanged(&self) {
		let curr = &self.base.join("curr");
		let sync = &self.base.join("sync");
		if !self.mfs.stat(curr).await.is_ok() {
			// WTF. Empty initial sync
			self.mfs.mkdir(&curr.join("data")).await.unwrap();
		} else {
			self.mfs.rm(&curr.join("meta")).await.unwrap();
		}
		self.mfs.cp(&sync.join("meta"), &curr.join("meta")).await.unwrap();
		self.mfs.rm_r(sync).await.unwrap();
	}
}
