use unmemftp::{ MemStorage, MemFile };
use ftp::FtpStream;

pub(crate) async fn memstream(files: Box<dyn Fn() -> MemStorage + Send + Sync>) -> FtpStream {
    let addr = unmemftp::serve(files).await;

    let mut ftp_stream = FtpStream::connect(addr).unwrap();
    ftp_stream.login("anonymous", "onymous").unwrap();
    ftp_stream.transfer_type(ftp::types::FileType::Binary).unwrap();
    return ftp_stream;
}

pub(crate) fn abc() -> MemStorage {
    MemStorage::new(vec![
        ("/a", MemFile::from_str("Test file 1")),
        ("/b/c", MemFile::from_str("Test file in a subdirectory")),
    ])
}
