# FTP2MFS

Syncs FTP folders into IPFS's mutable file system (the directory structure accessible through `ipfs files …`).

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

## Full configuration
```yaml
# The FTP server to copy from
source: ftp://…
/// Reprieve period for which files will be kept in MFS after deletion on server
reprieve: "1 month",
/// Ignore glob patterns when listing files on server (gitignore style)
ignore:
  # Example: Whitelist only folders a and b
  - "/*"
  - "!a"
  - "!b"

# FTP credentials, defaults to anonymous, can be overwritten from command line
user: anonymous
pass: example@example.example # Many FTP servers ask you to provide your e-mail address as password

# Path on MFS where the finalized sync result is kept
target: /publish-me
# Path on MFS where files are written to during sync - defaults to /temp/$hash_of_config
workdir: /temp/foobar
```
