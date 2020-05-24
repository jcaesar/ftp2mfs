use std::io::prelude::*;
use std::path::Path;

pub trait Provider {
	fn get(&self, p: &Path) -> Box<dyn Read + Send + Sync>;
}
