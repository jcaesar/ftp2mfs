use crate::alluse::*;

#[derive(Clone, Debug)]
/// Information about a file on the rsync server
pub struct File {
	/// Path as returned by the server
	/// Take care to normalize it
	pub path: Vec<u8>,
	/// Symlink target, if [is_symlink](crate::File::is_symlink)
	pub symlink: Option<Vec<u8>>,
	pub(crate) mode: u32,
	/// File size in bytes
	pub size: u64,
	/// Modification time. Range is limited as it is parsed from an i32
	pub mtime: Option<DateTime<Utc>>,
	/// Position in file list
	pub(crate) idx: usize,
	/// Since a different client may have a different file list and files are requested by index,
	/// not by path, we must ensure that files and clients don't get mixed.
	pub(crate) client_id: usize,
}
impl File {
	/// e.g. the `unix_mode` crate is useful for parsing
	pub fn unix_mode(&self) -> u32 {
		self.mode
	}
	/// A regular file?
	pub fn is_file(&self) -> bool {
		unix_mode::is_file(self.mode)
	}
	/// A directory?
	pub fn is_directory(&self) -> bool {
		unix_mode::is_dir(self.mode)
	}
	/// A symlink? `symlink` will be `Some`.
	pub fn is_symlink(&self) -> bool {
		unix_mode::is_symlink(self.mode)
	}
}
impl std::fmt::Display for File {
	/// Print as if we're doing `ls -l`
	fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
		write!(
			fmt,
			"{} {:>12} {} {}{}",
			unix_mode::to_string(self.mode),
			self.size,
			self.mtime
				.as_ref()
				.map(DateTime::to_rfc3339)
				.unwrap_or("                    ".to_owned()),
			String::from_utf8_lossy(&self.path),
			self.symlink
				.as_ref()
				.map(|s| format!(" -> {}", String::from_utf8_lossy(&s)))
				.unwrap_or("".to_owned())
		)?;
		Ok(())
	}
}

/// Rsync's file list entry "header"
pub struct FileEntryStatus(pub u8);
impl FileEntryStatus {
	pub fn is_end(&self) -> bool {
		self.0 == 0
	}
	#[allow(dead_code)] // I have really no idea what it is good for
	pub fn is_top_level(&self) -> bool {
		self.0 & 0x01 != 0
	}
	pub fn file_mode_repeated(&self) -> bool {
		self.0 & 0x02 != 0
	}
	pub fn uid_repeated(&self) -> bool {
		self.0 & 0x08 != 0
	}
	pub fn gid_repeated(&self) -> bool {
		self.0 & 0x10 != 0
	}
	pub fn inherits_filename(&self) -> bool {
		self.0 & 0x20 != 0
	}
	pub fn integer_filename_len(&self) -> bool {
		self.0 & 0x40 != 0
	}
	pub fn mtime_repeated(&self) -> bool {
		self.0 & 0x80 != 0
	}
}
