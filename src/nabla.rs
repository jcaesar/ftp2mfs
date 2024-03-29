use anyhow::{Context, Result};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SyncActs {
	pub meta: SyncInfo,
	pub delete: Vec<(PathBuf, bool)>,
	pub get: Vec<PathBuf>,
	pub sym: Vec<(PathBuf, PathBuf)>,
}
impl SyncActs {
	pub fn new(cur: SyncInfo, mut ups: SyncInfo, settings: &super::Settings) -> Result<SyncActs> {
		let reprieve = chrono::Duration::from_std(settings.reprieve).context(format!(
			"Outlandish reprieve duration specified: {:?}",
			settings.reprieve
		))?;

		// Calculate deleted files (and folders - don't keep empty folders)
		let mut deletes: HashMap<&Path, bool> = HashMap::new();
		for (f, i) in cur.files.iter() {
			if !ups.files.contains_key(f) {
				let now = Utc::now();
				let deleted = i.deleted.unwrap_or_else(|| now);
				if now.signed_duration_since(deleted) < reprieve && !ups.symlinks.contains_key(f) {
					ups.files.insert(
						f.clone(),
						FileInfo {
							deleted: Some(deleted),
							..*i
						},
					);
				} else {
					for a in PathAncestors::new(f) {
						deletes.insert(a, false /* hard */);
					}
				}
			}
		}
		for (s, _) in cur.symlinks.iter() {
			if !ups.symlinks.contains_key(s) {
				// TODO? reprieve for symlinks
				for a in PathAncestors::new(s) {
					deletes.insert(a, true /* soft */);
				}
			}
		}

		// Calculate adds and make sure no needed folders are deleted
		let mut gets: HashSet<&Path> = HashSet::new();
		for (f, i) in ups.files.iter() {
			if i.deleted.is_some() && cur.files.contains_key(f) {
				continue;
			}
			if !cur.files.get(f).filter(|existing| !existing.sync_required(i)).is_some() {
				gets.insert(&f);
			}
			for a in PathAncestors::new(f) {
				deletes.remove(a);
			}
		}
		let gets = gets;

		// As we removed add ancestors from the deletes, do the same for symlinks
		for k in ups.symlinks.keys() {
			for a in PathAncestors::new(k) {
				deletes.remove(a);
			}
		}

		// Finally, calculate optized set of paths to rm -r
		let optideletes: Vec<(PathBuf, bool)> = deletes
			.iter()
			.map(|(x, s)| (x.to_path_buf(), *s))
			.filter(|(d, _)| !PathAncestors::new(d).skip(1).any(|d| deletes.contains_key(d)))
			.collect();

		let added_symlinks = ups
			.symlinks
			.iter()
			.filter(|(k, v)| cur.symlinks.get(k.as_path()) != Some(v))
			.map(|(k, _)| k);

		let sym = crate::synlink::resolve(
			ups.symlinks.clone(),
			settings.max_symlink_cycle,
			deletes.keys().chain(gets.iter()).map(|p| *p),
			added_symlinks,
		);

		// Ret
		let gets = gets.into_iter().map(Path::to_path_buf).collect();
		Ok(SyncActs {
			meta: ups,
			delete: optideletes,
			get: gets,
			sym,
		})
	}

	pub fn stats(&self) -> Stats {
		Stats {
			final_bytes: self.meta.files.iter().filter_map(|(_, v)| v.s).sum(),
			get_bytes: self
				.get
				.iter()
				.filter_map(|p| {
					self.meta
						.files
						.get(p)
						.expect("trying to get a file, but don't know why")
						.s
				})
				.sum(),
			reprieve_files: self.meta.files.iter().filter(|(_, v)| v.deleted.is_some()).count(),
			reprieve_bytes: self
				.meta
				.files
				.iter()
				.filter(|(_, v)| v.deleted.is_some())
				.filter_map(|(_, v)| v.s)
				.sum(),
		}
	}
}

pub struct Stats {
	pub get_bytes: usize,
	pub final_bytes: usize,
	pub reprieve_files: usize,
	pub reprieve_bytes: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SyncInfo {
	version: u64,
	#[serde(serialize_with = "ordered_map")]
	pub files: HashMap<PathBuf, FileInfo>,
	#[serde(serialize_with = "ordered_map", default, skip_serializing_if = "HashMap::is_empty")]
	pub symlinks: HashMap<PathBuf, PathBuf>,
	#[serde(skip_serializing_if = "Option::is_none", default)]
	pub cid: Option<String>,
}

fn ordered_map<S, V>(value: &HashMap<PathBuf, V>, serializer: S) -> Result<S::Ok, S::Error>
where
	S: serde::Serializer,
	V: Serialize,
{
	let mut value: Vec<(&PathBuf, &V)> = value.iter().collect();
	value.sort_by(|(k1, _), (k2, _)| k1.cmp(&k2));
	use serde::ser::SerializeMap;
	let mut map = serializer.serialize_map(Some(value.len()))?;
	for (k, v) in value.iter() {
		map.serialize_entry(k, v)?;
	}
	map.end()
}

impl SyncInfo {
	pub fn new() -> SyncInfo {
		SyncInfo {
			version: 1,
			files: HashMap::new(),
			symlinks: HashMap::new(),
			cid: None,
		}
	}
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct FileInfo {
	pub t: Option<DateTime<Utc>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub deleted: Option<DateTime<Utc>>,
	pub s: Option<usize>,
	#[serde(skip_serializing, default)]
	pub solid: bool,
}

impl FileInfo {
	fn sync_required(&self, new: &FileInfo) -> bool {
		self.deleted.is_some() || (!new.solid && self != new)
	}
	pub fn found(t: Option<DateTime<Utc>>, s: Option<usize>) -> FileInfo {
		FileInfo {
			t,
			s,
			deleted: None,
			solid: false,
		}
	}
	pub fn solid() -> FileInfo {
		FileInfo {
			t: None,
			s: None,
			deleted: None,
			solid: true,
		}
	}
}

// Why did I write this? this exists in std…
// Maybe because it includes /
pub struct PathAncestors<'a> {
	p: Option<&'a Path>,
}
impl PathAncestors<'_> {
	pub fn new(p: &Path) -> PathAncestors {
		PathAncestors { p: Some(p) }
	}
}
impl<'a> Iterator for PathAncestors<'a> {
	type Item = &'a Path;
	fn next(&mut self) -> Option<&'a Path> {
		let mut next = self.p?.parent();
		std::mem::swap(&mut next, &mut self.p);
		return next;
	}
}

#[cfg(test)]
mod test {
	use super::*;

	fn threenew() -> SyncInfo {
		let mut new = SyncInfo::new();
		let dummyfai = || FileInfo::found(None, None);
		new.files.insert(Into::into("/a"), dummyfai());
		new.files.insert(Into::into("/b"), dummyfai());
		new.files.insert(Into::into("/c"), dummyfai());
		return new;
	}
	fn abc() -> HashSet<PathBuf> {
		stoset(&vec!["/a", "/b", "/c"])
	}

	fn toset(l: &[PathBuf]) -> HashSet<PathBuf> {
		l.iter().map(Clone::clone).collect()
	}
	fn stoset(l: &[&str]) -> HashSet<PathBuf> {
		l.iter().map(Into::into).collect()
	}

	fn exampleset(reprieve: u64) -> crate::Settings {
		crate::Settings {
			source: url::Url::parse("rsync:://liftm.de/arrsync/").unwrap(),
			reprieve: std::time::Duration::from_secs(reprieve),
			ignore: vec![],
			user: None,
			pass: None,
			workdir: None,
			target: "/mahmirr".into(),
			max_symlink_cycle: 3,
			solid: vec![],
		}
	}

	#[test]
	fn all_quiet_in_the_west() {
		let old = SyncInfo::new();
		let new = SyncInfo::new();
		let sa = SyncActs::new(old, new, &exampleset(0)).unwrap();
		assert!(sa.get.is_empty(), "{:?}", sa);
		assert!(sa.delete.is_empty(), "{:?}", sa);
	}

	#[test]
	fn no_new_is_good_new() {
		let old = threenew();
		let new = threenew();
		let sa = SyncActs::new(old, new, &exampleset(0)).unwrap();
		assert!(sa.get.is_empty(), "{:?}", sa);
		assert!(sa.delete.is_empty(), "{:?}", sa);
	}

	#[test]
	fn allnew_allsync() {
		let old = SyncInfo::new();
		let new = threenew();
		let sa = SyncActs::new(old, new, &exampleset(0)).unwrap();
		assert_eq!(toset(&sa.get), abc(), "Three new files: {:?}", sa);
		assert!(sa.delete.is_empty(), "No existing, no deletions: {:?}", sa);
	}

	#[test]
	fn allgone_alldelete() {
		let new = SyncInfo::new();
		let old = threenew();
		let sa = SyncActs::new(old, new, &exampleset(0)).unwrap();
		assert_eq!(
			toset(&sa.delete.iter().map(|(p, _)| p.to_owned()).collect::<Vec<_>>()),
			vec!["/"].into_iter().map(Into::into).collect::<HashSet<PathBuf>>(),
			"All deleted -> root deleted: {:?}",
			sa
		);
		assert!(sa.get.is_empty(), "No get {:?}", sa);
	}

	#[test]
	fn keep() {
		let new = SyncInfo::new();
		let old = threenew();
		let sa = SyncActs::new(old, new, &exampleset(1)).unwrap();
		assert!(sa.get.is_empty(), "No get {:?}", sa);
		assert!(sa.delete.is_empty(), "At first, it doesn't know when a file was deleted. So it should need to keep it, no matter how short the retention is: {:?}", sa);
	}
	#[test]
	fn then_delete() {
		let new = SyncInfo::new();
		let mut old = threenew();
		let now = Utc::now();
		for (_, i) in old.files.iter_mut() {
			i.deleted = Some(now - chrono::Duration::weeks(100 * 52));
		}
		let sa = SyncActs::new(old, new, &exampleset(1)).unwrap();
		assert!(sa.get.is_empty(), "No get {:?}", sa);
		assert_eq!(
			toset(&sa.delete.iter().map(|(p, _)| p.to_owned()).collect::<Vec<_>>()),
			stoset(&vec!["/"]),
			"All deleted -> root deleted: {:?}",
			sa
		);
	}

	#[test]
	fn info_changed() {
		let mut old = threenew();
		let mut new = threenew();
		let now = Utc::now();
		old.files.get_mut(&PathBuf::new().join("/a")).unwrap().s = Some(1);
		new.files.get_mut(&PathBuf::new().join("/a")).unwrap().s = Some(2);
		old.files.get_mut(&PathBuf::new().join("/b")).unwrap().t = Some(now);
		new.files.get_mut(&PathBuf::new().join("/b")).unwrap().t = Some(now + chrono::Duration::seconds(1));
		let sa = SyncActs::new(old, new, &exampleset(42)).unwrap();
		assert!(sa.delete.is_empty(), "Don't delete on change {:?}", sa);
		assert_eq!(toset(&sa.get), stoset(&vec!["/a", "/b"]), "Get changed: {:?}", sa);
	}

	#[test]
	fn honor_reprieve_on_recovery() {
		let old = SyncInfo::new();
		let mut new = threenew();
		let now = Utc::now();
		new.files.get_mut(&PathBuf::new().join("/a")).unwrap().deleted = Some(now);
		let sa = SyncActs::new(old, new, &exampleset(0)).unwrap();
		assert_eq!(
			toset(&sa.get),
			abc(),
			"A is marked get though the new has deleted set: {:?}",
			sa
		);
	}

	#[test]
	fn changed_symlinks() {
		let mut old = SyncInfo::new();
		let mut new = SyncInfo::new();
		old.symlinks.insert("a".into(), "gab".into());
		old.symlinks.insert("b".into(), "old".into());
		new.symlinks.insert("b".into(), "new".into());
		new.symlinks.insert("c".into(), "gaa".into());
		let sa = SyncActs::new(old, new, &exampleset(0)).unwrap();
		assert_eq!(
			sa.delete,
			vec![("a".into(), true)].into_iter().collect::<Vec<(PathBuf, bool)>>(),
			"delete for deleted symlink"
		);
		let mut sym = sa.sym.into_iter().map(|(_, t)| t).collect::<Vec<_>>();
		sym.sort();
		assert_eq!(sym, vec![PathBuf::from("b"), PathBuf::from("c")],);
	}

	#[test]
	fn optidelete_knows_symlinks() {
		let mut old = SyncInfo::new();
		let mut new = SyncInfo::new();
		old.symlinks.insert("x/a".into(), "gab".into());
		old.symlinks.insert("x/b".into(), "old".into());
		new.symlinks.insert("x/b".into(), "new".into());
		let sa = SyncActs::new(old, new, &exampleset(0)).unwrap();
		assert_eq!(
			sa.delete,
			vec![("x/a".into(), true)].into_iter().collect::<Vec<(PathBuf, bool)>>(),
			"Don't delete x or something..."
		);
	}
}
