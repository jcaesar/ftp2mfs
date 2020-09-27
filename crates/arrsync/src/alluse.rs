pub use anyhow::{Context, Result};
pub use bytes::{Bytes, BytesMut};
pub use chrono::prelude::*;
pub use core::task::Poll;
pub use std::marker::Unpin;
pub use std::path::Path;
pub use std::pin::Pin;
pub use tokio::io::AsyncBufReadExt;
pub use tokio::io::AsyncReadExt;
pub use tokio::io::AsyncWriteExt;
pub use tokio::io::{AsyncBufRead, AsyncRead, BufReader};
pub use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
pub use tokio::net::TcpStream;
pub fn default<T: Default>() -> T {
	std::default::Default::default()
} // default_free_fn
pub use crate::client::RsyncClient;
pub use crate::file::File;
pub use crate::file::FileEntryStatus;
pub use crate::receiver::ReadFilesProcess;
pub use std::collections::HashMap;
pub use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrder};
pub use std::sync::{Arc, Mutex as SyncMutex};
pub use tokio::sync::mpsc;
pub use tokio::sync::Mutex as AsyncMutex;
pub type Requests = Arc<SyncMutex<Option<HashMap<usize, Vec<mpsc::Sender<Result<Bytes, tokio::io::Error>>>>>>>;
pub type Enveloped = EnvelopeRead<BufReader<OwnedReadHalf>>;
pub use crate::envelope::EnvelopeRead;
pub use crate::envelope::RsyncReadExt;
