#![warn(clippy::pedantic)]

mod cmd;
mod config;
mod ingester;
mod query;
mod source;

use std::process::ExitCode;

use crate::cmd::run;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:?}");
            ExitCode::FAILURE
        }
    }
}
