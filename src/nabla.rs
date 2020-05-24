use chrono::prelude::*;
use serde::{ Deserialize, Serialize };
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{ PathBuf, Path };

#[derive(Debug)]
pub struct SyncActs {
	pub meta: SyncInfo,
	pub delete: Vec<PathBuf>,
	pub get: Vec<PathBuf>,
}
impl SyncActs {
	pub fn new(cur: SyncInfo, mut ups: SyncInfo, reprieve: std::time::Duration) -> SyncActs {
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
pub struct SyncInfo {
	version: u64,
	pub files: HashMap<PathBuf, FileInfo>,
	#[serde(default = "Utc::now")]
	pub lastsync: DateTime<Utc>,
}

impl SyncInfo {
	pub fn new() -> SyncInfo { SyncInfo {
		version: 1,
		files: HashMap::new(),
		lastsync: Utc::now(),
	}}
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct FileInfo {
	pub t: Option<DateTime<Utc>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub deleted: Option<DateTime<Utc>>,
	pub s: Option<usize>
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
