use std::collections::{HashMap, HashSet};
use std::path::Component;
use std::path::{Path, PathBuf};

/// Generates a list of copy operations that emulates a set of symlinks, while maximally traversing
/// cycles n times
pub fn resolve<P1, PI1, P2, PI2>(
	mut m: HashMap<PathBuf, PathBuf>,
	n: u64,
	changes: P1,
	added_symlinks: P2,
) -> Vec<(PathBuf, PathBuf)>
where
	P1: Iterator<Item = PI1>,
	PI1: AsRef<Path>,
	P2: Iterator<Item = PI2>,
	PI2: AsRef<Path>,
{
	cleanup_weirds(&mut m);
	let m = resolve_links(m);
	/* Now we got a list of symlinks that don't contain other symlinks in their path
	 * These can be emulated as shallow copies.
	 * Problem is: if a copy target it a subpath of a copy source,
	 * there is an order dependency between the two.
	 * So, we arrange the symlinks into a directed graph with an edge for each dependency.
	 * If there were no cycles, we could just arrange them in topological order.
	 * Instead, I implement a post-order multi-traversal with limited repetitions */
	// Don't get confused here. A node in this graph is a symlink, i.e. a source and target.
	// However, the nodes are uniquely identified by their source, so I only keep those

	// As a preparation, construct a map of symlink targets (copy sources) and where they're linked from
	let mrev = m.iter().fold(HashMap::new(), |mut mrev, (k, v)| {
		mrev.entry(v).or_insert(vec![]).push(k);
		mrev
	});

	// Second, find the edges of the modification graph, in source -> [destination] representaiton
	// If a symlink T's target is the ancestor of another symlink S's source (copy target), S should be done before T.
	let edges: HashMap<&Path, Vec<&Path>> = m
		.iter()
		.flat_map(|(k, _)| k.ancestors().map(move |a| (k.as_path(), a)))
		.flat_map(|(child, ancestor)| {
			mrev.get(&ancestor.to_owned())
				.iter()
				.flat_map(|dependee| dependee.iter())
				.map(move |dependee| (child.clone(), dependee.clone()))
				.collect::<Vec<(_, _)>>()
		})
		.fold(HashMap::new(), |mut edges, (k, v)| {
			edges.entry(k).or_insert(vec![]).push(v);
			edges
		});
	log::trace!("Order dependencies: {:#?}", edges);

	// Finally, do a max-n-repetitions postorder traversal
	let mut visited: HashMap<&Path, u64> = HashMap::new();
	let mut previous: HashSet<&Path> = HashSet::new();
	let mut stack: Vec<&Path> = vec![];
	let mut run = vec![];
	let mut trav = vec![];
	let mut visit = |e: &Path| {
		if previous.contains(e) {
			return;
		}
		let e = m.get_key_value(e).unwrap().0; // the element from the iterator dies too quickly, so get the equivalent from m
		log::trace!("Entering symlink graph at {:?}", e);
		stack.push(e);
		while let Some(e) = stack.pop() {
			if previous.contains(e) {
				continue;
			}
			let dst = m.get(&e.to_owned()).unwrap();
			let visited = visited.entry(e).or_insert(0);
			if *visited > n {
				let level = if n > 0 { log::Level::Debug } else { log::Level::Warn };
				log::log!(
					level,
					"{:?} -> {:?} is part of a symlink cycle, stopping at depth {}",
					e,
					dst,
					n
				);
				previous.insert(e); // Don't warn twice.
				continue;
			}
			run.push((dst, e));
			*visited += 1u64;
			stack.append(&mut edges.get(e).unwrap_or(&vec![]).clone());
			// TODO: rotate the edges vector, or sort it by visit count
		}
		trav.reserve(run.len());
		for (f, t) in run.drain(..).rev() {
			trav.push((f.to_owned(), t.to_owned()));
		}
		for (k, _) in visited.drain() {
			previous.insert(k);
		}
	};
	for p in changes {
		for er in p.as_ref().ancestors() {
			for es in mrev.get(&er.to_path_buf()) {
				for e in es {
					visit(e);
				}
			}
		}
	}
	for s in added_symlinks {
		let s = s.as_ref();
		if m.contains_key(s) {
			visit(s);
		}
	}
	trav
}

/// Evict absolute paths that leave "." and some other odd stuff
fn cleanup_weirds(m: &mut HashMap<PathBuf, PathBuf>) {
	let evict: Vec<_> = m.iter().filter_map(|(k, v)| {
        let ar = k.ancestors().skip(1).all(|anc| match m.get(anc) {
                Some(anct) => {
                    log::error!("Symlink {:?} -> {:?} is shadowed by {:?} -> {:?} - symlinks should be at absolute paths, so one can't contain the other. (Evicting {:?} and continuing despite insanity.)", k, v, anc, anct, k);
                    false
                },
                None => true
            });
		let kr = k.components().all(|e| match e {
			Component::Normal(_) => true,
			_ => false,
		});
		if !kr {
			log::debug!("Evicting {:?} -> {:?}: strange component in origin path", k, v);
		}
		let vr = v.components().all(|e| match e {
			Component::RootDir => false,
			Component::Prefix(_) => false,
			_ => true,
		});
		if !vr {
			log::debug!("Evicting {:?} -> {:?}: absolute symlink", k, v);
		}
		match kr && vr && ar {
           true => None,
           false => Some(k.to_owned()),
        }
	}).collect();
	for k in evict.into_iter() {
		m.remove(&k);
	}
}

/// Resolves symlinks so that their target paths don't contain other symlinks
/// Unresoveable / cyclic links are evicted
fn resolve_links(m: HashMap<PathBuf, PathBuf>) -> HashMap<PathBuf, PathBuf> {
	let m = m
		.iter()
		.filter_map(|(k, v)| {
			let mut position = k.to_owned();
			let mut todo = vec![];
			let mut used = HashMap::new();
			loop {
				if let Some(ld) = m.get(&position) {
					if let Some(old) = used.insert(position.to_owned(), todo.len()) {
						if old <= todo.len() {
							log::debug!("Evicting {:?} -> {:?}: circle at {:?}", k, v, ld);
							return None;
							// The idea is that it is ok to step through symlinks multiple time,
							// but we have to make progress.
							// (see resresres test)
							// TODO: Give this some formal analysis
							// Are symlinks a stack machine?
						}
					}
					position.pop();
					todo.append(&mut ld.components().rev().collect::<Vec<_>>());
				}
				match todo.pop() {
					None => return Some((k.to_owned(), position)),
					Some(Component::Normal(push)) => {
						position.push(push);
						// Technically speaking, I have to check whether that directory exists
						// But I don't track the existence of directories...
						// I guess it's ok to skip that.
						// There's no(?) real point in having non-resolving symlinks behave correctly
					}
					Some(Component::ParentDir) => match position.pop() {
						true => (),
						false => {
							log::debug!("Evicting {:?} -> {:?}: leaving root", k, v);
							return None;
						}
					},
					Some(Component::CurDir) => (),
					Some(Component::Prefix(_)) => unreachable!(),
					Some(Component::RootDir) => unreachable!(),
				}
			}
		})
		.collect();
	log::trace!("Resolved symlinks: {:#?}", m);
	m
}

#[cfg(test)]
mod test {
	use super::*;

	fn gset<T: std::iter::FromIterator<(PathBuf, PathBuf)>>(l: &[(&str, &str)]) -> T {
		l.into_iter()
			.map(|(s, d)| (Path::new(s).to_owned(), Path::new(d).to_owned()))
			.collect()
	}
	fn set(l: &[(&str, &str)]) -> HashMap<PathBuf, PathBuf> {
		gset(l)
	}
	fn list(l: &[(&str, &str)]) -> Vec<(PathBuf, PathBuf)> {
		gset(l)
	}
	fn sort<T: Ord>(mut v: Vec<T>) -> Vec<T> {
		v.sort();
		v
	}

	fn nopaths() -> impl Iterator<Item = &'static Path> {
		std::iter::empty()
	}

	#[test]
	fn dropweirds() {
		let mut weird = set(&[("/a", ""), ("..", ""), ("./.", ""), ("ok", "/")]);
		cleanup_weirds(&mut weird);
		assert_eq!(weird, set(&[]));
	}

	#[test]
	fn resresres() {
		let weird = set(&[("a", "."), ("b", "a/a/a/c")]);
		let weird = resolve_links(weird);
		assert_eq!(weird, set(&[("a", ""), ("b", "c")]));
	}

	#[test]
	fn circdelink() {
		let weird = set(&[("a", "a"), ("b", "c"), ("c", "b")]);
		let weird = resolve_links(weird);
		assert_eq!(weird, set(&[]));
	}

	#[test]
	fn undefineddirectorybehavior() {
		// This required a directory b/c to exist. But we can't check that.
		// So we let it slide.
		let weird = set(&[("a", "b/c/..")]);
		let weird = resolve_links(weird);
		assert_eq!(weird, set(&[("a", "b")]));
	}

	#[test]
	fn chain() {
		let l = set(&[("a", "b"), ("b", "c")]);
		let weird = resolve(l.clone(), 0, vec![Path::new("c")].iter(), nopaths());
		// In this case, the two symlinks don't have a dependency on each other, so we need to sort
		assert_eq!(sort(weird), sort(list(&[("c", "b"), ("c", "a")])));
	}

	#[test]
	fn aaaa() {
		let l = set(&[("a", ".")]);
		let weird = resolve(l.clone(), 2, nopaths(), l.keys());
		assert_eq!(weird, list(&[("", "a"), ("", "a"), ("", "a")]));
	}

	#[test]
	fn feight() {
		let l = set(&[("a", "."), ("b", ".")]);
		let weird = resolve(l.clone(), 1, vec![Path::new("c")].iter(), nopaths());
		// The order of the result is a little bit undefined… :/
		assert_eq!(sort(weird), sort(list(&[("", "a"), ("", "a"), ("", "b"), ("", "b")])));
	}

	#[test]
	fn sub() {
		let l = set(&[("a/b", "../d"), ("c", "a")]);
		let weird = resolve(l.clone(), 2, vec![Path::new("d")].iter(), nopaths());
		assert_eq!(weird, list(&[("a", "c"), ("d", "a/b")]));
	}

	#[test]
	fn oneoftwo() {
		let l = set(&[("a", "b"), ("c", "d")]);
		let weird = resolve(l.clone(), 42, vec![Path::new("b/asdf")].iter(), nopaths());
		assert_eq!(weird, list(&[("b", "a")]));
	}
}
