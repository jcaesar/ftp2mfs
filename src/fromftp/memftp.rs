use async_ftp::FtpStream;
use unmemftp::{MemFile, MemStorage};

pub(crate) async fn memstream(files: Box<dyn Fn() -> MemStorage + Send + Sync>) -> FtpStream {
	let addr = unmemftp::serve(files).await;

	let mut ftp_stream = FtpStream::connect(addr).await.unwrap();
	ftp_stream.login("anonymous", "onymous").await.unwrap();
	ftp_stream
		.transfer_type(async_ftp::types::FileType::Binary)
		.await
		.unwrap();
	return ftp_stream;
}

pub(crate) fn abc() -> MemStorage {
	MemStorage::new(vec![
		("/a", MemFile::from_str("Test file 1")),
		("/b/c", MemFile::from_str("Test file in a subdirectory")),
	])
}
