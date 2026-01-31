#![warn(clippy::pedantic)]
#![allow(clippy::cast_possible_truncation)]

mod cmd;
mod config;
mod ingester;
mod query;
mod source;

use std::process::ExitCode;

use anyhow::Result;
use env_logger::{Builder, Env};

use crate::cmd::run;

fn main() -> Result<ExitCode> {
    Builder::from_env(Env::new().filter("KIFA_LOG")).format_target(true).init();

    run()
}
