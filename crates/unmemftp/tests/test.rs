use ftp::FtpStream;
use std::result::Result;
use unmemftp::{ MemStorage, MemFile, serve };
use std::io::Read;

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

		let ftp = &mut default_ftp_client(addr);
		retr_test(ftp, "/a", "A");
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn accesspath_no_threaded_scheduler() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr);
		retr_test(ftp, "/b/c", "BC");
	}

	#[tokio::test(threaded_scheduler)]
	pub async fn cd_and_get() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr);
		ftp.cwd("b").unwrap();
		retr_test(ftp, "c", "BC");
	}
	
	#[tokio::test(threaded_scheduler)]
	pub async fn cannot_cd_to_nowhere() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr);
		assert!(ftp.cwd("d").is_err());
	}
	
	#[tokio::test(threaded_scheduler)]
	pub async fn cannot_get_nowhere() {
		let addr = serve(Box::new(somefiles)).await;

		let ftp = &mut default_ftp_client(addr);
		assert!(ftp.simple_retr("d").is_err());
	}

	fn retr_test(ftp: &mut FtpStream, path: &str, expected: &str) {
		ftp.retr(path, |r| {
			assert_eq!(std::str::from_utf8(&r.bytes().collect::<Result<Vec<u8>, _>>().unwrap()).unwrap(), expected);
			Ok(())
		}).unwrap();
	}
}

mod acro {
	use super::*;
	use std::sync::Arc;
	use libunftp::auth::{ Authenticator, DefaultUser };
	use std::error::Error;

	struct BobAuthenticator {}
	#[async_trait::async_trait]
	impl Authenticator<DefaultUser> for BobAuthenticator {
		async fn authenticate(&self, username: &str, password: &str) -> Result<DefaultUser, Box<dyn Error + Send + Sync>> {
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

		default_ftp_client(addr.parse().unwrap());
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

		let mut ftp = FtpStream::connect(addr).unwrap();
		ftp.login("bob", "bob").unwrap();
		ftp.transfer_type(ftp::types::FileType::Binary).unwrap();
		assert!(ftp.simple_retr("a").is_ok());
	}
}
	
fn default_ftp_client(addr: std::net::SocketAddr) -> FtpStream { 
	let mut ftp = FtpStream::connect(addr).unwrap();
	ftp.login("anonymous", "onymous").unwrap();
	ftp.transfer_type(ftp::types::FileType::Binary).unwrap();
	return ftp;
}
