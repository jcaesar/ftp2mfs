# Asynchronous Retrieving Rust rSYNc Client

[![Crates.io](https://img.shields.io/crates/v/arrsync.svg)](https://crates.io/crates/arrsync)
[![Docs.rs](https://docs.rs/arrsync/badge.svg)](https://docs.rs/arrsync/)

A tokio-based rsync wire protocol client library for listing files on and downloading from rsyncd servers.

### Status
Incredibly alpha. It seems to do the thing, which is: retrieve a list of files on the server, and then retrieve some of those files whole.
But don't look at it sharply…

### Limitations
 * Rsyncd only, no execute-over-ssh.
 * Rsync protocol 27.0 only (Fortunately, newer rsyncds do fallbacks.)
 * Dates are parsed as i32. (I hope this library and maybe even rsync are dead by 2038…)
 * The MOTD and error messages are `log`ed, but can't otherwise be intercepted.
 * Protocol error handling is probably flawed.
 * Use of `anyhow`.
 * No tests

### Related crates
 * [rsyn](https://crates.io/crates/rsyn)
   is probably closest in functionality, implements an rsync wire protocol client. Sadly, the library is aimed directly at implementing an rsync client CLI clone, so getting a `Read` for the retrieved files is not possible.
 * [rrsync](https://crates.io/crates/rrsync)
   implements its own wire protocol(?).
 * [rsync-list](https://crates.io/crates/rsync-list)
   seems to parse the output of executing `rsync`. Brr.
 * [librsync-sys](https://crates.io/crates/librsync-sys), [librsync-ffi](https://crates.io/crates/librsync-ffi), [fast_rsync](https://crates.io/crates/fast_rsync)
   only do the delta calculation. arrsync may depend on them in the future.
 * [libsyncr](https://crates.io/crates/librsyncr)
   remains a mystery to me.
