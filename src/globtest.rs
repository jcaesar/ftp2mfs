// Just making sure I understand how ignore works

use crate::globlist;

#[test]
fn basic() {
	let gs = globlist(vec!["a"]).unwrap();
	assert!(gs.matched("a", false).is_ignore());
	assert!(!gs.matched("b", false).is_ignore());
}

#[test]
fn negations() {
	let gs = globlist(vec!["!a"]).unwrap();
	assert!(!gs.matched("!a", false).is_ignore());
	assert!(!gs.matched("a", false).is_ignore());
	let gs = globlist(vec!["*", "!a"]).unwrap();
	assert!(gs.matched("b", false).is_ignore());
	assert!(!gs.matched("a", false).is_ignore());
}

#[test]
fn globs() {
	let gs = globlist(vec!["*"]).unwrap();
	assert!(gs.matched("a", false).is_ignore());
	assert!(gs.matched("b", false).is_ignore());
}

#[test]
fn unignore_subdir() {
	let gs = globlist(vec!["*", "!b/*"]).unwrap();
	assert!(gs.matched("b", false).is_ignore());
	assert!(gs.matched("b", true).is_ignore());
	assert!(!gs.matched("b/c", false).is_ignore());
	assert!(gs.matched("b/c/d", false).is_ignore());
}

#[test]
fn unignore_subdir_only() {
	let gs = globlist(vec!["*", "!b/"]).unwrap();
	assert!(gs.matched("b", false).is_ignore());
	assert!(!gs.matched("b", true).is_ignore());
	assert!(gs.matched("b/c", false).is_ignore());
}

#[test]
fn unignore_recursively() {
	let gs = globlist(vec!["*", "!b/**"]).unwrap();
	assert!(gs.matched("b", false).is_ignore());
	assert!(gs.matched("b", true).is_ignore());
	assert!(!gs.matched("b/c", true).is_ignore());
	assert!(!gs.matched("b/c", true).is_ignore());
}

#[test]
fn ignore_unrecursively() {
	let gs = globlist(vec!["/*", "!b"]).unwrap();
	assert!(!gs.matched("b", false).is_ignore());
	assert!(!gs.matched("b", true).is_ignore());
	assert!(!gs.matched("b/c", true).is_ignore());
	assert!(!gs.matched("b/c", true).is_ignore());
}

#[test]
fn over() {
	let gs = globlist(vec!["a*", "!ab*", "abc*"]).unwrap();
	assert!(gs.matched("aaad", false).is_ignore());
	assert!(!gs.matched("abad", false).is_ignore());
	assert!(gs.matched("abcd", false).is_ignore());
}
