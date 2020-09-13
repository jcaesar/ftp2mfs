# FTP2MFS

Syncs folders into IPFS's mutable file system (the directory structure accessible through `ipfs files …`).
Despite the name, FTP2MFS is capable of reading from FTP, HTTP directory listings, rsyncd, and the local filesystem.

## Usage
You need to create a configuration file that specifies what to sync, e.g. minimally:
```yaml
# FTP server to read from
source: ftp://ftp.jaist.jp/pub/Linux/ArchLinux/core/os/x86_64/
# Beware that many FTP servers don't do well with IPv6 (Or maybe the FTP library ftp2mfs uses doesn't). If in doubt, specify the server by IP address, or change /etc/gai.conf to prefer IPv4, or …

# MFS folder to write to. The actual mirrored files will be written to $target/data
target: /some-archlinux-core-mirror
```

Save the file as `ftp2mfs-cfg1` and run `ftp2mfs --config ftp2mfs-cfg1` (or `cargo run --` if you did not `cargo install` this).
FTP2MFS will first materialize the files in some folder in `/temp`, and make a copy at `$target` once it successfully completes the sync.
If the operation fails, restarting with the same configuration file should continue the operations.
Note that source folder structures are fully explored before any copying/downloading is done, so it may take a while for any files to be copied.

## Sources
All source types have their advantages and drawbacks. Building nice mirrors is difficult.
### HTTP
* (+) Is the only encryptable/secured source available.
* (-) There is no defined format for listing directories and directory structure is guessed from HTML: Links ending with / are treated as directories, others as files. Links that do not point to direct children of the current directory are ignored.
* (-) One HEAD request has to be sent for each file to see if it is up to date. This is somewhat inefficient
* (0) It might be nice to parse modification dates and file sizes from HTML directory listings
### FTP
* (+) Proper standardized protocol for listing directories and transferring files
* (-) FTPs not supported (it is supported by the underlying library and would be easy to implement)
* (-) Many FTP servers don't support the MLSD command. To find out whether a file is up to date, FTP2MFS has to send one CWD, one MDTM, and one SIZE command per file sequentially. This is incredibly inefficient.
* (0) Support may be added for parsing some of the most common human directory listing formats
### Rsync
* (+) Efficient sync with full metadata
* (-) [Self-baked implementation](./crates/arrsync) of a protocol with no standardization and little documentation
* (-) Not secured
### Local filesystem
* (+) The only implementation where syncing is trivial and thus probably correctly implemented
* (-) You'll have the files twice, once on your filesystem and once in your IPFS repo (No filestore/urlstore support)
* (0) Might be interesting in combination with FUSE

## Full configuration format
```yaml
# The server to copy from
source: ftp://…|rsync://…|…
# Reprieve period for which files will be kept in MFS after deletion on server
reprieve: "1 month"
# Ignore glob patterns when listing files on server (gitignore style)
ignore:
  # Example: Whitelist only folders a and b
  - "/*"
  - "!a"
  - "!b"
  # setting RUST_LOG=debug as environment variable may help with debugging ignore rules

# FTP credentials, defaults to anonymous, can be overwritten from command line
user: anonymous
pass: example@example.example # Many FTP servers ask you to provide your e-mail address as password

# Path on MFS where the finalized sync result is kept
target: /publish-me
# Path on MFS where files are written to during sync - defaults to /temp/$hash_of_config
workdir: /temp/foobar
```
