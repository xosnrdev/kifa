#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    unused
)]

pub(crate) mod buffer;
pub(crate) mod compaction;
pub mod engine;
pub(crate) mod helpers;
pub(crate) mod manifest;
pub mod memtable;
pub(crate) mod sstable;
pub(crate) mod wal;

pub const KIBI: usize = 1024; // 1KB

pub const MEBI: usize = KIBI * KIBI; // 1MB

pub mod common {
    use std::path::{Path, PathBuf};
    use std::{fs, io};

    use crate::helpers::sync_dir;

    #[macro_export]
    macro_rules! map_err {
        ($variant:ident, $err_ty:ty) => {
            impl From<$err_ty> for Error {
                fn from(err: $err_ty) -> Self {
                    Error::$variant(err)
                }
            }
        };
    }

    #[must_use]
    pub fn temp_path(final_path: &Path) -> PathBuf {
        let mut temp = final_path.as_os_str().to_owned();
        temp.push(".tmp");
        PathBuf::from(temp)
    }

    pub fn atomic_rename(from: &Path, to: &Path) -> io::Result<()> {
        sync_dir(to)?;
        fs::rename(from, to)?;
        sync_dir(to)
    }
}

/// Controls when data is fsync'd to disk.
///
/// | Mode | Behavior | Data at Risk |
/// |------|----------|--------------|
/// | `Normal` | Batch sync every ~50 writes | Up to 50 entries |
/// | `Cautious` | Sync after each write | None (after return) |
/// | `Emergency` | Sync immediately, pause compaction | None |
///
/// Throughput depends on storage hardware. Consumer SSDs typically achieve
/// 100-300 fsyncs/sec; enterprise `NVMe` with power-loss protection can exceed 30,000.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    /// Batch syncs for throughput.
    ///
    /// Data is written immediately but only fsync'd every ~50 writes.
    /// Up to 50 entries may be lost on power failure.
    Normal,

    /// Sync after every write for durability.
    Cautious,

    /// Maximum durability for imminent power loss.
    ///
    /// Like `Cautious`, but also pauses background compaction to reduce I/O.
    Emergency,
}
