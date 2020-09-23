use async_ftp::FtpStream;
use std::result::Result;
use unmemftp::{serve, MemFile, MemStorage};

fn somefiles() -> MemStorage {
	MemStorage::new(vec![
		("/a", MemFile::from_str("A")),
		("/b/c", MemFile::from_str("BC")), // Containing folders are automatically created - no support for empty
	])
}

mod serve {
	use super::*;

	#[tokio::test(threaded_scheduler)]
	pub async fn accessdirect() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr).await;
		retr_test(ftp, "/a", "A").await;
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn accesspath_no_threaded_scheduler() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr).await;
		retr_test(ftp, "/b/c", "BC").await;
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn cd_and_get() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr).await;
		ftp.cwd("b").await.unwrap();
		retr_test(ftp, "c", "BC").await;
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn cannot_cd_to_nowhere() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr).await;
		assert!(ftp.cwd("d").await.is_err());
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn cannot_get_nowhere() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr).await;
		assert!(ftp.simple_retr("d").await.is_err());
	}

	async fn retr_test(ftp: &mut FtpStream, path: &str, expected: &str) {
		use async_ftp::{DataStream, FtpError};
		use tokio::io::{AsyncReadExt, BufReader};
		async fn receive(mut reader: BufReader<DataStream>) -> Result<String, FtpError> {
			let mut buffer = Vec::new();
			reader
				.read_to_end(&mut buffer)
				.await
				.map_err(FtpError::ConnectionError)?;
			return Ok(std::str::from_utf8(&buffer).unwrap().to_owned());
		}
		assert_eq!(ftp.retr(path, receive).await.unwrap(), expected);
	}
}

mod acro {
	use super::*;
	use libunftp::auth::{Authenticator, DefaultUser};
	use std::error::Error;
	use std::sync::Arc;

	#[derive(Debug)]
	struct BobAuthenticator {}
	#[async_trait::async_trait]
	impl Authenticator<DefaultUser> for BobAuthenticator {
		async fn authenticate(
			&self,
			username: &str,
			password: &str,
		) -> Result<DefaultUser, Box<dyn Error + Send + Sync>> {
			if username == "bob" && password == "bob" {
				Ok(DefaultUser {})
			} else {
				Err("bye!".into())
			}
		}
	}

	// For advanced configurations, start unftp manually
	#[tokio::test(threaded_scheduler)]
	#[should_panic]
	pub async fn youre_not_bob() {
		let addr = "127.0.0.1:46858";
		let server = libunftp::Server::new(Box::new(somefiles))
			.greeting("irrelevant")
			.authenticator(Arc::new(BobAuthenticator {}))
			.passive_ports(50000..65535);
		tokio::spawn(server.listen(addr));
		unmemftp::connectable(addr.parse().unwrap()).await;

		default_ftp_client(addr.parse().unwrap()).await;
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn you_are_bob() {
		let addr = "127.0.0.1:46859";
		let server = libunftp::Server::new(Box::new(somefiles))
			.greeting("irrelevant")
			.authenticator(Arc::new(BobAuthenticator {}))
			.passive_ports(50000..65535);
		tokio::spawn(server.listen(addr));
		unmemftp::connectable(addr.parse().unwrap()).await;

		let mut ftp = FtpStream::connect(addr).await.unwrap();
		ftp.login("bob", "bob").await.unwrap();
		ftp.transfer_type(async_ftp::types::FileType::Binary).await.unwrap();
		assert!(ftp.simple_retr("a").await.is_ok());
	}
}

async fn default_ftp_client(addr: std::net::SocketAddr) -> FtpStream {
	let mut ftp = FtpStream::connect(addr).await.unwrap();
	ftp.login("anonymous", "onymous").await.unwrap();
	ftp.transfer_type(async_ftp::types::FileType::Binary).await.unwrap();
	return ftp;
}
