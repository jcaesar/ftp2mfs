// Just making sure I understand how ignore works

use crate::ignore;

#[test]
fn basic() {
    let gs = ignore(vec!["a"]).unwrap();
    assert!(gs.matched("a", false).is_ignore());
    assert!(!gs.matched("b", false).is_ignore());
}

#[test]
fn negations() {
    let gs = ignore(vec!["!a"]).unwrap();
    assert!(!gs.matched("!a", false).is_ignore());
    assert!(!gs.matched("a", false).is_ignore());
    let gs = ignore(vec!["*", "!a"]).unwrap();
    assert!(gs.matched("b", false).is_ignore());
    assert!(!gs.matched("a", false).is_ignore());
}

#[test]
fn globs() {
    let gs = ignore(vec!["*"]).unwrap();
    assert!(gs.matched("a", false).is_ignore());
    assert!(gs.matched("b", false).is_ignore());
}

#[test]
fn unignore_subdir() {
    let gs = ignore(vec!["*", "!b/*"]).unwrap();
    assert!(gs.matched("b", false).is_ignore());
    assert!(gs.matched("b", true).is_ignore());
    assert!(!gs.matched("b/c", false).is_ignore());
    assert!(gs.matched("b/c/d", false).is_ignore());
}

#[test]
fn unignore_subdir_only() {
    let gs = ignore(vec!["*", "!b/"]).unwrap();
    assert!(gs.matched("b", false).is_ignore());
    assert!(!gs.matched("b", true).is_ignore());
    assert!(gs.matched("b/c", false).is_ignore());
}

#[test]
fn unignore_recursively() {
    let gs = ignore(vec!["*", "!b/**"]).unwrap();
    assert!(gs.matched("b", false).is_ignore());
    assert!(gs.matched("b", true).is_ignore());
    assert!(!gs.matched("b/c", true).is_ignore());
    assert!(!gs.matched("b/c", true).is_ignore());
}

#[test]
fn ignore_unrecursively() {
    let gs = ignore(vec!["/*", "!b"]).unwrap();
    assert!(!gs.matched("b", false).is_ignore());
    assert!(!gs.matched("b", true).is_ignore());
    assert!(!gs.matched("b/c", true).is_ignore());
    assert!(!gs.matched("b/c", true).is_ignore());
}
