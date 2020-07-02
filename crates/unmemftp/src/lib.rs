use libunftp::storage::Error as FtpError;
use libunftp::storage::Result as FtpResult;
use libunftp::storage::StorageBackend;
use libunftp::storage::{ Metadata, Fileinfo, ErrorKind };
use std::collections::HashMap;
use std::io::Cursor;
use std::net::{ SocketAddr, TcpListener };
use std::path::{ Path, PathBuf };
use std::time::SystemTime;
use std::time::{ Duration, Instant };
use tokio::net::TcpStream;

pub async fn serve(files: Box<dyn Fn() -> MemStorage + Send + Sync>) -> SocketAddr {
	let addr = {
		// Not exactly race-free, but libunftp doesn't seem to have a method for listening on a free port
		// There's ways around this (SO_REUSEPORT should do, eg, but… meh)
		let listener = TcpListener::bind("127.0.0.1:0").expect("listening on any 127.0.0.1 port");
		listener.local_addr().expect("getting local listening address")
	};

	let server = libunftp::Server::new(files)
		.greeting("irrelevant")
		.passive_ports(50000..65535);
	tokio::spawn(server.listen(format!("{}:{}", addr.ip(), addr.port())));

    connectable(addr).await;
    return addr;
}

pub async fn connectable(addr: SocketAddr) {
	let deadline = Instant::now() + Duration::from_secs(15);
	loop {
		tokio::time::delay_for(Duration::from_millis(50)).await;
		if TcpStream::connect(addr).await.is_ok() {
			return;
		}
		if Instant::now() > deadline {
			panic!("libunftp not accepting connections");
		}
	}
}

#[derive(Clone, Debug)]
pub struct MemFile {
	content: MemFileType,
	modified: SystemTime,
}
#[derive(Clone)]
pub enum MemFileType {
	Dir,
	File(Vec<u8>)
}
impl std::fmt::Debug for MemFileType {
	fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>)
		-> Result<(), std::fmt::Error>
	{
		match self {
			MemFileType::Dir => fmt.debug_struct("MemFileType::Dir").finish(),
			MemFileType::File(content) => fmt.debug_struct("MemFileType::File")
				.field("content", &format!("[ Content of len {}: {:?}, ...]", content.len(), &content[..10] ))
				.finish(),
		}
	}
}
impl MemFile {
	pub fn from_str(c: &str) -> MemFile { MemFile {
		content: MemFileType::File(c.as_bytes().to_owned()),
		modified: SystemTime::now(),
	}}
}

#[derive(Clone,Debug)]
pub struct MemStorage {
	files: HashMap<PathBuf, MemFile>,
}
impl MemStorage {
	pub fn new<S,D>(files: D) -> MemStorage 
		where D: AsRef<[(S, MemFile)]>, S: AsRef<str>
	{ MemStorage {
		files: files.as_ref().iter().flat_map(|(p, f)| Path::new(p.as_ref())
			.ancestors()
			.skip(1)
			.map(|pa| 
				 (pa.to_path_buf(), MemFile { 
					 modified: f.modified, 
					 content: MemFileType::Dir 
				 })
			)
			.chain(std::iter::once((Path::new(p.as_ref()).to_path_buf(), f.clone())))
			.collect::<Vec<(PathBuf, MemFile)>>() // Meh
		).collect(),
	}}
	fn deny<O>() -> FtpResult<O> { Err(FtpError::from(ErrorKind::PermissionDenied)) }
	fn find<P: AsRef<Path>>(&self, p: P) -> FtpResult<&MemFile> {
		match self.files.get(p.as_ref()) {
			Some(entry) => Ok(entry),
			None => Err(FtpError::from(ErrorKind::PermanentFileNotAvailable)),
		}
	}
}

#[derive(Debug)]
pub struct MemMetadata {
	 modified: SystemTime,
	 typ: MemMetadataType,
}
impl From<&MemFile> for MemMetadata {
	fn from(f: &MemFile) -> Self { MemMetadata {
		modified: f.modified,
		typ: MemMetadataType::from(&f.content),
	}}
}
#[derive(Debug)]
pub enum MemMetadataType {
	File { size: u64 },
	Dir,
}
impl From<&MemFileType> for MemMetadataType {
	fn from(t: &MemFileType) -> Self { match t { 
		MemFileType::Dir => MemMetadataType::Dir, 
		MemFileType::File(content) => MemMetadataType::File { size: content.len() as u64 },
	}}
}
impl Metadata for MemMetadata {
	fn len(&self) -> u64 { match &self.typ { MemMetadataType::File { size, .. } => *size, MemMetadataType::Dir => 42 /* Traditionally, folders are 4096 bytes large. Wonder what this will break */ } }
	fn is_dir(&self) -> bool { !self.is_file() }
	fn is_file(&self) -> bool { match &self.typ { MemMetadataType::File { .. } => true, MemMetadataType::Dir => false } }
	fn is_symlink(&self) -> bool { false } // Nah
	fn modified(&self) -> Result<std::time::SystemTime, FtpError> { Ok(self.modified) }
	fn gid(&self) -> u32 { 1042 }
	fn uid(&self) -> u32 { 1042 }
}

#[async_trait::async_trait]
impl<U: Send + Sync + std::fmt::Debug> StorageBackend<U> for MemStorage {
	type File = Cursor<Vec<u8>>;
	type Metadata = MemMetadata;

	async fn metadata<P: AsRef<Path> + Send>(&self, _user: &Option<U>, path: P) -> FtpResult<Self::Metadata> {
		Ok(self.find(path)?.into())
	}

	async fn list<P>(&self, _user: &Option<U>, path: P) -> FtpResult<Vec<Fileinfo<PathBuf, Self::Metadata>>>
	where
		P: AsRef<Path> + Send,
		<Self as StorageBackend<U>>::Metadata: Metadata,
	{
		let path = path.as_ref();
		match &self.find(path)?.content {
			MemFileType::Dir => (),
			MemFileType::File(_) => return Err(FtpError::from(ErrorKind::PermanentFileNotAvailable)),
		};
		Ok(self.files.iter()
		   .filter(|(p, _)| p.parent() == Some(path))
		   .map(|(p, i)| Fileinfo { path: p.to_path_buf(), metadata: i.into() })
		   .collect()
		)
	}

	async fn get<P: AsRef<Path> + Send>(&self, _user: &Option<U>, path: P, start_pos: u64) -> FtpResult<Self::File> {
		match &self.find(path)?.content {
			MemFileType::Dir => Err(FtpError::from(ErrorKind::PermanentFileNotAvailable)),
			MemFileType::File(content) => Ok(Cursor::new(content[start_pos as usize..].to_vec())),
		}
	}
	
	async fn cwd<P: AsRef<Path> + Send>(&self, _user: &Option<U>, path: P) -> FtpResult<()> {
		match &self.find(path)?.content {
			MemFileType::Dir => Ok(()),
			MemFileType::File(_) => Err(FtpError::from(ErrorKind::LocalError)),
		}
	}

	async fn put<P: AsRef<Path> + Send, R: tokio::io::AsyncRead + Send + Sync + 'static + Unpin>(&self,	_user: &Option<U>, _bytes: R, _path: P, _start_pos: u64) -> FtpResult<u64> { Self::deny() }
	async fn del<P: AsRef<Path> + Send>(&self, _user: &Option<U>, _path: P) -> FtpResult<()> { Self::deny() }
	async fn rmd<P: AsRef<Path> + Send>(&self, _user: &Option<U>, _path: P) -> FtpResult<()> { Self::deny() }
	async fn mkd<P: AsRef<Path> + Send>(&self, _user: &Option<U>, _path: P) -> FtpResult<()> { Self::deny() }
	async fn rename<P: AsRef<Path> + Send>(&self, _user: &Option<U>, _from: P, _to: P) -> FtpResult<()> { Self::deny() }
}
