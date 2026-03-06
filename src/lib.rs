pub mod account;
pub mod engine;
pub mod reader;
pub mod router;
pub mod transaction;

use std::error::Error;
use std::path::PathBuf;

pub fn err_msg(message: &str) -> Box<dyn Error> {
    message.into()
}

pub fn input_path() -> Result<PathBuf, Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    if let Some(path) = args.next() {
        return Ok(PathBuf::from(path));
    }
    Err(err_msg("no input path provided"))
}
