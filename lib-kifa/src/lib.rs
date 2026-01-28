#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    unused
)]

#[macro_use]
pub mod helpers;
pub(crate) mod buffer;
pub(crate) mod compaction;
pub(crate) mod manifest;
pub mod memtable;
pub(crate) mod sstable;
pub(crate) mod wal;
