use chrono::prelude::*;
use clap::Clap;
use ftp::FtpStream;
use ftp::types::FileType;
use serde::{ Deserialize, Serialize };
use std::collections::HashMap;
use std::path::{ PathBuf, Path };
use std::collections::HashSet;
use ipfs_api::IpfsClient;
use std::io::prelude::*;
use std::io::Cursor;
use crossbeam_channel::{ unbounded, bounded };
use std::thread;
use crossbeam_channel::{ Sender, Receiver };

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
	println!("{:?}", opts);

	let out = ToMfs::new(&opts.api, Path::new(&opts.base).to_path_buf());
	out.prepare().await;

	let mut ftp_stream = FtpStream::connect(format!("{}:21", opts.connect)).unwrap();
	ftp_stream.login(&opts.user, &opts.pass).unwrap();
	ftp_stream.transfer_type(FileType::Binary).unwrap();

	for ref cwd in opts.cwd {
		ftp_stream.cwd(cwd).unwrap();
	}

	let cur = out.get_last_state().await;
	let ups = Recursor::run(&mut ftp_stream);
	let sa = SyncActs::new(cur, ups, *opts.reprieve);

	println!("{:#?}", sa);
	out.apply(sa, &FtpProvider::new(ftp_stream)).await;
}



trait Provider {
	fn get(&self, p: &Path) -> Box<dyn Read + Send + Sync>;
}

struct FtpProvider {
	mkreq: Sender<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>,
}
impl FtpProvider {
	fn new(mut ftp: FtpStream) -> FtpProvider {
		let (sender, receiver) = unbounded::<(PathBuf, Sender<Result<Vec<u8>, std::io::Error>>)>();
		thread::spawn(move || {
			while let Ok((path, chan)) = receiver.recv() {
				let retr = ftp.retr(path.to_str().unwrap(), |r| {
					let mut bufsize = 1<<16;
					loop {
						let mut buf = Vec::new();
						buf.resize(bufsize, 0);
						match r.read(&mut buf) {
							Ok(n) => {
								buf.truncate(n);
								if !chan.send(Ok(buf)).is_ok() {
									break;
								}
								if n == 0 {
									break;
								}
								bufsize = std::cmp::max(n * 9 / 8, 4096);
							},
							Err(e) => match e.kind() {
									std::io::ErrorKind::Interrupted => continue,
									_ => { chan.send(Err(e)).ok(); break }
							}
						}

					}
					Ok(())
				});
				if let Err(e) = retr {
					chan.send(Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))).ok();
				}
			}
			ftp.quit().unwrap();
		});
		FtpProvider { mkreq: sender } 
	}
}

// IterRead won't do, we need to own that channel
struct ChannelReader {
	receiver: Receiver<Result<Vec<u8>, std::io::Error>>,
	current: Cursor<Vec<u8>>,
}
impl Read for ChannelReader {
	fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
		let read = self.current.read(buf)?;
		if read == 0 {
			let nextbuf = self.receiver.recv()
				.map_err(|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))??;
			if nextbuf.len() == 0 {
				return Ok(0);
			}
			std::mem::swap(&mut self.current, &mut Cursor::new(nextbuf));
			return self.current.read(buf);
		} else {
			Ok(read)
		}
	}
}

impl Provider for FtpProvider {
	fn get(&self, p: &Path) -> Box<dyn Read + Send + Sync> {
		let (sender, receiver) = bounded(128);
		self.mkreq.send((p.to_path_buf(), sender)).unwrap();
		Box::new(ChannelReader { receiver, current: Cursor::new(vec![]) })
	}
}

struct ToMfs {
	mfs: Mfs,
	base: PathBuf,
}

impl ToMfs {
	fn new(api: &str, base: PathBuf) -> ToMfs { ToMfs {
		mfs: Mfs::new(api),
		base: base,
	}}
	async fn prepare(&self) {
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

		self.mfs.emplace(&sync.join("lastsync"), Cursor::new(serde_json::to_vec(&Utc::now()).unwrap())).await.unwrap();
		// "Lock"
		let pid = Cursor::new(format!("{}", std::process::id()));
		self.mfs.emplace(pidfile, pid).await.unwrap();
	}
	async fn get_last_state(&self) -> SyncInfo {
		let meta = &self.base.join("curr").join("meta");
		match self.mfs.stat(meta).await {
			Ok(_) => serde_json::from_slice(&self.mfs.read_fully(meta).await.unwrap()).unwrap(),
			Err(_) => SyncInfo::default(),
		}
	}
	async fn apply(&self, sa: SyncActs, p: &dyn Provider) {
		// TODO: desequentialize
		let sync = &self.base.join("sync");
		let syncdata = sync.join("data");

		let SyncActs { meta, get, delete } = sa;

		self.mfs.emplace(&sync.join("meta"), Cursor::new(serde_json::to_vec(&meta).unwrap())).await.unwrap();

		for d in delete.iter() {
			self.mfs.rm_r(&syncdata.join(d)).await.unwrap();
		}
		for a in get.iter() {
			let pth = &syncdata.join(&a);
			self.mfs.mkdirs(pth.parent().unwrap()).await.unwrap();
			self.mfs.emplace(pth, p.get(&a)).await.unwrap();
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
			self.mfs.rm(&curr.join("lastsync")).await.unwrap();
		}
		self.mfs.cp(&sync.join("meta"),     &curr.join("meta")    ).await.unwrap();
		self.mfs.cp(&sync.join("lastsync"), &curr.join("lastsync")).await.unwrap();
		self.mfs.rm_r(sync).await.unwrap();
	}
}

pub struct Mfs {
	ipfs: IpfsClient,
}
impl Mfs {
	pub fn new(api: &str) -> Mfs { Mfs {
		ipfs: ipfs_api::TryFromUri::from_str(api).unwrap(),
	}}
	pub async fn stat(&self, p: &Path)
		-> Result<ipfs_api::response::FilesStatResponse, ipfs_api::response::Error>
		{ self.ipfs.files_stat(p.to_str().unwrap()).await }
	pub async fn rm_r(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_rm(p.to_str().unwrap(), true).await }
	pub async fn rm(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_rm(p.to_str().unwrap(), false).await }
	pub async fn mkdirs(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_mkdir(p.to_str().unwrap(), true).await }
	pub async fn mkdir(&self, p: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_mkdir(p.to_str().unwrap(), false).await }
	pub async fn mv(&self, s: &Path, d: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_mv(s.to_str().unwrap(), d.to_str().unwrap()).await }
	pub async fn cp(&self, s: &Path, d: &Path)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_cp(s.to_str().unwrap(), d.to_str().unwrap()).await }
	pub async fn emplace<R: 'static + Read + Send + Sync>(&self, d: &Path, data: R)
		-> Result<(), ipfs_api::response::Error>
		{ self.ipfs.files_write(d.to_str().unwrap(), true, true, data).await }
	pub fn read(&self, s: &Path)
		-> impl futures_core::stream::Stream<Item = Result<bytes::Bytes, ipfs_api::response::Error>>
		{ self.ipfs.files_read(s.to_str().unwrap()) }

	pub async fn read_fully(&self, s: &Path) -> Result<Vec<u8>, ipfs_api::response::Error> {
		use futures_util::stream::TryStreamExt;
		self.read(s)
			.map_ok(|chunk| chunk.to_vec())
		    .try_concat()
			.await
		// Optimally, I'd have a version of this that returns a Read or similar…
	}
}

pub struct PathAncestors<'a> {
	p: Option<&'a Path>,
}
impl PathAncestors<'_> {
	pub fn new(p: &Path) -> PathAncestors { PathAncestors { p: Some(p) } }
}
impl <'a> Iterator for PathAncestors<'a> {
	type Item = &'a Path;
	fn next(&mut self) -> Option<&'a Path> {
		let mut next = self.p?.parent();
		std::mem::swap(&mut next, &mut self.p);
		return next;
	}
}

pub fn id<T>(x: T) -> T { x }

#[derive(Debug)]
struct SyncActs {
	meta: SyncInfo,
	delete: Vec<PathBuf>,
	get: Vec<PathBuf>,
}
impl SyncActs {
	fn new(cur: SyncInfo, mut ups: SyncInfo, reprieve: std::time::Duration) -> SyncActs {
		let reprieve = chrono::Duration::from_std(reprieve).unwrap();
		// Calculate deleted files (and folders - don't keep empty folders)
		let mut deletes: HashSet<&Path> = HashSet::new();
		for (f, i) in cur.files.iter() {
			if !ups.files.contains_key(f) {
				let now = Utc::now();
				let deleted = i.deleted.unwrap_or_else(|| now);
				if now.signed_duration_since(deleted) < reprieve {
					ups.files.insert(f.clone(), FileInfo { deleted: Some(deleted), ..*i });
				} else {
					for a in PathAncestors::new(f) {
						deletes.insert(a);
					}
				}
			}
		};

		// Calculate adds and make sure no needed folders are deleted
		let mut gets: HashSet<&Path> = HashSet::new();
		for (f, i) in ups.files.iter() {
			if i.deleted.is_some() {
				continue;
			}
			if !cur.files.get(f).filter(|existing| &i == existing).is_some() {
				gets.insert(&f);
			}
			for a in PathAncestors::new(f) {
				deletes.remove(a);
			}
		}
		let gets = gets;

		// Finally, calculate optized set of paths to rm -r
		let deletes: Vec<PathBuf> =
			deletes.iter().map(|x| *x)
			.filter(|d| !PathAncestors::new(d).skip(1).any(|d| deletes.contains(d)))
			.map(Path::to_path_buf)
			.collect();

		// Ret
		let gets = gets.into_iter().map(Path::to_path_buf).collect();
		return SyncActs { meta: ups, delete: deletes, get: gets };
	}
}

#[derive(Serialize, Deserialize, Debug)]
struct SyncInfo {
	version: u64,
	files: HashMap<PathBuf, FileInfo>
}

impl Default for SyncInfo {
	fn default() -> SyncInfo { SyncInfo {
		version: 1,
		files: HashMap::new()
	}}
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct FileInfo {
	t: Option<DateTime<Utc>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	deleted: Option<DateTime<Utc>>,
	s: Option<usize>
}

//impl PartialEq for FileInfo {
//	fn eq(&self, other: &FileInfo) -> bool {
//		self.s == other.s
//	}
//	fn ne(&self, other: &FileInfo) -> bool { !self.eq(other) }
//}

struct Recursor<'a> {
	ftp: &'a mut FtpStream,
	base: PathBuf,
	result: SyncInfo,
}
impl <'a> Recursor <'a> {
	pub fn run(ftp: &'a mut FtpStream) -> SyncInfo {
		let wd = ftp.pwd().unwrap();
		let mut r = Recursor {
			ftp,
			base: Path::new(&wd).to_path_buf(),
			result: Default::default()
		};
		r.rec();
		r.result
	}
	fn rec(&mut self) {
		let pth = Path::new(&self.ftp.pwd().unwrap()).to_path_buf();
		match self.ftp.nlst(None) {
			Ok(lst) => for ref f in lst {
				match self.ftp.cwd(f) {
					Ok(()) => {
						self.rec();
						self.ftp.cdup().unwrap();
					},
					Err(_) => {
						let name = pth.clone().join(f);
						let name = pathdiff::diff_paths(name, self.base.clone()).unwrap();
						self.result.files.insert(name, FileInfo {
							t: self.ftp.mdtm(&f).ok().flatten(),
							s: self.ftp.size(&f).ok().flatten(),
							deleted: None,
						});
					},
				}
			},
			Err(e) => println!("ERR {:?} {:?}", pth, e)
		}
	}
}
