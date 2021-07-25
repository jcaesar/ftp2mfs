//! A tokio-based, rust only, self baked, partial rsync wire protocol implementation for retrieving
//! files from rsyncd servers.
//!
//! Quick example:
//! ```no_run
//! // Print all the Manifest files for gentoo ebuilds
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let url = url::Url::parse("rsync://rsync.de.gentoo.org/gentoo-portage/")?;
//!     let (mut client, files) = arrsync::RsyncClient::connect(&url).await?;
//!     for file in files.into_iter() {
//!         if !file.is_file() {
//!             continue;
//!         }
//!         if !file.path.ends_with("/Manifest".as_bytes()) {
//!             continue;
//!         }
//!         async fn get_and_print(
//!             file: arrsync::File,
//!             client: arrsync::RsyncClient
//!         ) -> anyhow::Result<()> {
//!             use tokio::io::AsyncReadExt;
//!             let mut content = vec![];
//!             Box::pin(client.get(&file).await?).read_to_end(&mut content).await?;
//!             // "Race" to finish. Ignored for simplicity reasons.
//!             print!("{}", String::from_utf8(content)?);
//!             Ok(())
//!         }
//!         tokio::spawn(get_and_print(file, client.clone()));
//!     }
//!     client.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! Beware that rsync is, of course, unencrypted.

// Meh. Guess I should have given the File a reference to the client, and put the get function on
// there. Sounds more natural now. Oh well.

mod alluse;
mod client;
mod envelope;
mod file;
mod receiver;

pub use client::RsyncClient;
pub use file::File;
