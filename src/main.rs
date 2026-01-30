#![warn(clippy::pedantic)]
#![allow(clippy::cast_possible_truncation)]

mod cmd;
mod config;
mod ingester;
mod query;
mod source;

use std::process::ExitCode;

use env_logger::{Builder, Env};

use crate::cmd::run;

fn main() -> ExitCode {
    Builder::from_env(Env::new().filter("KIFA_LOG")).format_target(true).init();

    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:?}");
            ExitCode::FAILURE
        }
    }
}
