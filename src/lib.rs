pub mod account;
pub mod engine;
pub mod reader;
pub mod router;
pub mod transaction;

use std::error::Error;
use std::io::BufReader;
use std::fs::File;

pub fn err_msg(message: &str) -> Box<dyn Error> {
    message.into()
}

pub fn input_reader() -> Result<BufReader<File>, Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    if let Some(path) = args.next() {
        let file = File::open(path)?;
        return Ok(BufReader::with_capacity(1 << 20, file));
    }
    Err(err_msg("no input path provided"))
}