use chrono::prelude::*;
use serde::{ Deserialize, Serialize };
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{ PathBuf, Path };
use anyhow::{ Result, Context };

#[derive(Debug)]
pub struct SyncActs {
	pub meta: SyncInfo,
	pub delete: Vec<PathBuf>,
	pub get: Vec<PathBuf>,
}
impl SyncActs {
	pub fn new(cur: SyncInfo, mut ups: SyncInfo, reprieve: std::time::Duration) -> Result<SyncActs> {
		let reprieve = chrono::Duration::from_std(reprieve)
            .with_context(|| format!("Outlandish reprieve duration specified: {:?}", reprieve))?;
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
		Ok(SyncActs { meta: ups, delete: deletes, get: gets })
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

#[cfg(test)]
mod test {
    use super::*;

    fn threenew() -> SyncInfo {
        let mut new = SyncInfo::new();
        let dummyfai = || FileInfo { t: None, s: None, deleted: None };
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

    #[test]
    fn all_quiet_in_the_west() {
        let old = SyncInfo::new();
        let new = SyncInfo::new();
        let sa = SyncActs::new(old, new, std::time::Duration::from_secs(0)).unwrap();
        assert!(sa.get.is_empty(), "{:?}", sa);
        assert!(sa.delete.is_empty(), "{:?}", sa);
    }

    #[test]
    fn no_new_is_good_new() {
        let old = threenew();
        let new = threenew();
        let sa = SyncActs::new(old, new, std::time::Duration::from_secs(0)).unwrap();
        assert!(sa.get.is_empty(), "{:?}", sa);
        assert!(sa.delete.is_empty(), "{:?}", sa);
    }

    #[test]
    fn allnew_allsync() {
        let old = SyncInfo::new();
        let new = threenew();
        let sa = SyncActs::new(old, new, std::time::Duration::from_secs(0)).unwrap();
        assert_eq!(toset(&sa.get), abc(), "Three new files: {:?}", sa);
        assert!(sa.delete.is_empty(), "No existing, no deletions: {:?}", sa);
    }

    #[test]
    fn allgone_alldelete() {
        let new = SyncInfo::new();
        let old = threenew();
        let sa = SyncActs::new(old, new, std::time::Duration::from_secs(0)).unwrap();
        assert_eq!(toset(&sa.delete), vec!["/"].into_iter().map(Into::into).collect::<HashSet<PathBuf>>(), "All deleted -> root deleted: {:?}", sa);
        assert!(sa.get.is_empty(), "No get {:?}", sa);
    }

    #[test]
    fn keep() {
        let new = SyncInfo::new();
        let old = threenew();
        let sa = SyncActs::new(old, new, std::time::Duration::from_secs(1)).unwrap();
        assert!(sa.get.is_empty(), "No get {:?}", sa);
        assert!(sa.delete.is_empty(), "At first, it doesn't know when a file was deleted. So it should need to keep it, no matter how short the retention is: {:?}", sa);
    }
    #[test]
    fn then_delete() {
        let new = SyncInfo::new();
        let mut old = threenew();
        let now = Utc::now();
        for (_,i) in old.files.iter_mut() {
            i.deleted = Some(now - chrono::Duration::weeks(100 * 52));
        }
        let sa = SyncActs::new(old, new, std::time::Duration::from_secs(1)).unwrap();
        assert!(sa.get.is_empty(), "No get {:?}", sa);
        assert_eq!(toset(&sa.delete), stoset(&vec!["/"]), "All deleted -> root deleted: {:?}", sa);
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
        let sa = SyncActs::new(old, new, std::time::Duration::from_secs(42)).unwrap();
        assert!(sa.delete.is_empty(), "Don't delete on change {:?}", sa);
        assert_eq!(toset(&sa.get), stoset(&vec!["/a", "/b"]), "Get changed: {:?}", sa);
    }
}
