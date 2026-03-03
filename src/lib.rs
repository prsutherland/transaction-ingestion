pub mod account;
pub mod engine;
pub mod transaction;

use std::error::Error;

pub fn err_msg(message: &str) -> Box<dyn Error> {
    message.into()
}
