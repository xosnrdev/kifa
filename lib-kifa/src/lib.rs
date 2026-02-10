#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::str::FromStr;
use std::sync::Arc;

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
/// | `Normal` | Batch fsync every ~50 writes | Up to 49 entries |
/// | `Cautious` | fsync after each write | None (after return) |
/// | `Emergency` | fsync immediately, pause compaction | None |
///
/// Throughput depends on storage hardware. Consumer SSDs typically achieve
/// 100-300 fsyncs/sec; enterprise `NVMe` with power-loss protection can exceed 30,000.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    /// Batches fsync calls for maximum throughput at the cost of durability.
    ///
    /// Data is written immediately via `O_DIRECT` but only fsync'd every 50 writes.
    /// On power failure, up to 49 entries that were written but not yet fsync'd
    /// may be lost. The 50th write triggers an fsync that makes all 50 durable.
    ///
    /// **Warning**
    ///
    /// This mode trades durability for throughput. Use only when losing recent
    /// entries is acceptable. For financial or audit-critical data, use `Cautious`.
    Normal = 0,

    /// Fsyncs after every write for immediate durability.
    ///
    /// Each [`StorageEngine::append`](crate::engine::StorageEngine::append)
    /// returns only after the entry is fsync'd to stable storage. No data loss
    /// occurs on power failure for entries where `append()` returned successfully.
    ///
    /// This is the recommended default for financial and audit-critical workloads.
    Cautious = 1,

    /// Maximum durability mode for imminent power failure.
    ///
    /// Behaves like `Cautious` (fsync every write) but also pauses background
    /// compaction to minimize disk I/O. Use when brown-out detection triggers
    /// or UPS signals low battery. Escalate to this mode via SIGUSR1.
    Emergency = 2,
}

impl FromStr for FlushMode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "normal" => Ok(FlushMode::Normal),
            "cautious" => Ok(FlushMode::Cautious),
            "emergency" => Ok(FlushMode::Emergency),
            _ => Err("invalid flush mode"),
        }
    }
}

impl FlushMode {
    /// Returns true if this mode fsyncs after every write.
    #[must_use]
    pub const fn is_durable(self) -> bool {
        matches!(self, Self::Cautious | Self::Emergency)
    }
}

impl TryFrom<u8> for FlushMode {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Normal),
            1 => Ok(Self::Cautious),
            2 => Ok(Self::Emergency),
            _ => Err("invalid flush mode value"),
        }
    }
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
    pub data: Arc<[u8]>,
}

impl Entry {
    /// Returns the in-memory size of this entry in bytes.
    ///
    /// Includes the LSN (8 bytes), timestamp (8 bytes), and data length.
    /// This is the logical size, not the on-disk size which includes
    /// checksums and alignment padding.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        size_of::<u64>() + size_of::<u64>() + self.data.len()
    }
}
