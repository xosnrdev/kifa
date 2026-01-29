#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

mod buffer;
mod compaction;
pub mod engine;
mod helpers;
mod manifest;
mod memtable;
mod sstable;
mod wal;

pub const KIBI: usize = 1024; // 1KiB

pub const MEBI: usize = KIBI * KIBI; // 1MiB

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
    ///
    /// Each [`StorageEngine::append`](crate::engine::StorageEngine::append)
    /// returns only after fsync completes.
    Cautious,

    /// Maximum durability for imminent power loss.
    ///
    /// Like `Cautious`, but also pauses background compaction to reduce I/O.
    Emergency,
}

/// A single log entry retrieved from storage.
///
/// Each entry represents one [`StorageEngine::append`] call that was successfully persisted.
/// Entries are immutable once written and maintain strict LSN ordering.
///
/// # Fields
///
/// - `lsn`: Log sequence number, monotonically increasing across all entries.
/// - `timestamp_ms`: Unix timestamp in milliseconds when the entry was written.
/// - `data`: The raw bytes that were appended.
///
/// [`StorageEngine::append`]: crate::engine::StorageEngine::append
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Log sequence number assigned at write time.
    ///
    /// LSNs are globally unique and strictly increasing across all entries.
    pub lsn: u64,

    /// Unix timestamp in milliseconds when the entry was persisted.
    ///
    /// Wall-clock time from the writing process, not a logical clock.
    /// The `lsn` field provides ordering guarantees.
    pub timestamp_ms: u64,

    /// The raw payload bytes, stored as-is without interpretation.
    pub data: Vec<u8>,
}

impl Entry {
    /// Returns the in-memory size of this entry in bytes.
    ///
    /// Includes the LSN (8 bytes), timestamp (8 bytes), and data length.
    /// This is the logical size, not the on-disk size which includes
    /// checksums and alignment padding.
    #[must_use]
    pub const fn size_bytes(&self) -> usize {
        size_of::<u64>() + size_of::<u64>() + self.data.len()
    }
}
