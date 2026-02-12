//! Crash-proof storage engine for append-only log workloads.
//!
//! The engine follows the LSM-tree pattern but orders entries by LSN rather than key. A simple
//! `Vec` replaces the sorted memtable because append-only workloads never require key lookups.
//! This design targets POS and mobile money systems where durability matters more than random
//! access. Emergency mode flushes the memtable immediately when power loss is imminent.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{self, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{fmt, io};

use crate::compaction::{self, commit_compaction, prepare_compaction, run_compaction};
use crate::helpers::HeapEntry;
use crate::manifest::{self, Manifest, SstableEntry};
use crate::memtable::Memtable;
use crate::sstable::{self, SstableInfo, SstableIter, SstableReader};
use crate::wal::{self, Wal, WalStats};
use crate::{Entry, FlushMode, MEBI, map_err};

const DEFAULT_MEMTABLE_FLUSH_THRESHOLD: usize = 4 * MEBI;
const DEFAULT_COMPACTION_THRESHOLD: usize = 4;
const COMPACTION_POLL_INTERVAL: Duration = Duration::from_secs(5);
const LOCK_FILE_NAME: &str = ".lock";

/// Errors that can occur during storage engine operations.
///
/// Most errors are recoverable; the engine remains in a consistent state and
/// can continue processing. The exception is I/O errors during fsync, which
/// cause a panic (data integrity cannot be guaranteed).
#[derive(Debug)]
pub enum Error {
    /// Write-ahead log error (append, sync, or recovery failed).
    Wal(wal::Error),
    /// `SSTable` error (flush or read failed).
    Sstable(sstable::Error),
    /// Manifest error (metadata persistence failed).
    Manifest(manifest::Error),
    /// Background compaction error.
    Compaction(compaction::Error),
    /// Underlying I/O error.
    Io(io::Error),
    /// Another process holds the directory lock.
    ///
    /// Only one [`StorageEngine`] instance can open a directory at a time.
    /// This prevents data corruption from concurrent access.
    DirectoryLocked,
}

map_err!(Wal, wal::Error);
map_err!(Sstable, sstable::Error);
map_err!(Manifest, manifest::Error);
map_err!(Compaction, compaction::Error);
map_err!(Io, io::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wal(e) => write!(f, "wal: {e}"),
            Self::Sstable(e) => write!(f, "sstable: {e}"),
            Self::Manifest(e) => write!(f, "manifest: {e}"),
            Self::Compaction(e) => write!(f, "compaction: {e}"),
            Self::Io(e) => e.fmt(f),
            Self::DirectoryLocked => write!(f, "directory is locked by another process"),
        }
    }
}

impl std::error::Error for Error {}

/// Configuration for the storage engine.
///
/// All fields have sensible defaults via [`Config::default()`].
///
/// # Examples
///
/// ```
/// use lib_kifa::engine::Config;
///
/// // The default configuration suits most workloads.
/// let config = Config::default();
///
/// // Custom thresholds reduce memory usage on constrained devices.
/// let config = Config {
///     memtable_flush_threshold: 1024 * 1024,
///     compaction_threshold: 2,
///     compaction_enabled: true,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// Flush memtable to `SSTable` when it reaches this size in bytes.
    ///
    /// Lower values reduce memory usage but increase I/O. Default: 4 MiB.
    pub memtable_flush_threshold: usize,

    /// Trigger compaction when `SSTable` count reaches this threshold.
    ///
    /// Lower values keep read performance high but increase write amplification.
    /// Default: 4.
    pub compaction_threshold: usize,

    /// Whether to run background compaction.
    ///
    /// Disable for emergency situations or when I/O bandwidth is critical.
    /// Default: `true`.
    pub compaction_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memtable_flush_threshold: DEFAULT_MEMTABLE_FLUSH_THRESHOLD,
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            compaction_enabled: true,
        }
    }
}

/// Report of what happened during [`StorageEngine::open`].
///
/// This provides visibility into the recovery process. A clean shutdown
/// typically shows zero entries replayed; a crash recovery shows how much
/// data was recovered from the WAL.
///
/// # Examples
///
/// ```no_run
/// # use std::path::Path;
/// # use lib_kifa::engine::{StorageEngine, Config};
/// let (engine, report) = StorageEngine::open(Path::new("/data"), Config::default()).unwrap();
///
/// if report.wal_entries_replayed > 0 {
///     eprintln!("Recovered {} entries from crash", report.wal_entries_replayed);
/// }
///
/// if !report.gaps.is_empty() {
///     eprintln!("Warning: {} gaps in LSN sequence", report.gaps.len());
/// }
/// ```
#[derive(Debug)]
pub struct RecoveryReport {
    /// Number of WAL entries replayed into the memtable.
    ///
    /// Zero after a clean shutdown; nonzero after crash recovery.
    pub wal_entries_replayed: usize,

    /// LSN up to which data is persisted in `SSTables`.
    ///
    /// Entries with LSN <= this value are safely on disk.
    pub checkpoint_lsn: u64,

    /// First LSN replayed from WAL, if any.
    pub first_replayed_lsn: Option<u64>,

    /// Last LSN replayed from WAL, if any.
    pub last_replayed_lsn: Option<u64>,

    /// Timestamp of first replayed entry (milliseconds since Unix epoch).
    pub first_timestamp_ms: Option<u64>,

    /// Timestamp of last replayed entry (milliseconds since Unix epoch).
    pub last_timestamp_ms: Option<u64>,

    /// Gaps in the LSN sequence (indicates data loss).
    ///
    /// Each range represents missing LSNs. Empty if no gaps detected.
    pub gaps: Vec<Range<u64>>,

    /// Number of `SSTables` found on disk.
    pub sstable_count: usize,

    /// Number of temporary files cleaned up.
    ///
    /// Leftover `.tmp` files from interrupted operations.
    pub temp_files_cleaned: usize,

    /// Number of orphaned `SSTables` removed.
    ///
    /// `SSTables` not referenced in the manifest (from interrupted compaction).
    pub orphan_sstables_cleaned: usize,
}

/// Current state of the storage engine.
///
/// Useful for monitoring and debugging. All values are point-in-time
/// snapshots and may change immediately after reading.
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    /// WAL statistics (LSN counters, flush mode).
    pub wal: WalStats,

    /// Current memtable size in bytes.
    pub memtable_size_bytes: usize,

    /// Number of entries in the memtable.
    pub memtable_entry_count: usize,

    /// Number of `SSTables` on disk.
    pub sstable_count: usize,

    /// Highest LSN persisted to `SSTable`s.
    ///
    /// Entries with LSN <= this are durable even without WAL.
    pub checkpoint_lsn: u64,
}

struct Inner {
    memtable: Memtable,
    manifest: Manifest,
}

struct CompactionState {
    handle: Option<JoinHandle<()>>,
    trigger: Arc<(Mutex<bool>, Condvar)>,
    shutdown: Arc<AtomicBool>,
    emergency: Arc<AtomicBool>,
}

/// Crash-proof storage engine for log entries.
///
/// `StorageEngine` provides durable, ordered storage for arbitrary byte sequences.
/// It uses a write-ahead log (WAL) for durability and an LSM-tree structure for
/// efficient reads.
///
/// # Durability Guarantees
///
/// When [`append`](Self::append) returns successfully:
/// - The entry is written to the WAL
/// - In [`FlushMode::Cautious`] or [`FlushMode::Emergency`], it is also fsync'd
/// - In [`FlushMode::Normal`], fsync happens within the next 50 writes
///
/// After a crash, all fsync'd entries are recovered automatically.
///
/// # Thread Safety
///
/// `StorageEngine` is `Send` but not `Sync`. For concurrent access, wrap in
/// `Arc<Mutex<StorageEngine>>` or use a single writer with multiple readers
/// via [`snapshot`](Self::snapshot).
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use lib_kifa::FlushMode;
/// use lib_kifa::engine::{Config, StorageEngine};
///
/// // Opens the storage directory and recovers any uncommitted entries.
/// let (engine, report) = StorageEngine::open(Path::new("/var/lib/myapp"), Config::default())
///     .expect("failed to open");
///
/// // Cautious mode syncs each write to disk before returning.
/// engine.set_flush_mode(FlushMode::Cautious);
///
/// // The append call blocks until the entry is durable on disk.
/// let lsn = engine.append(b"TXN-001: payment received").unwrap();
/// println!("Stored with LSN {lsn}");
///
/// // Retrieves the entry by its assigned LSN.
/// if let Some(entry) = engine.get(lsn).unwrap() {
///     println!("Data: {}", String::from_utf8_lossy(&entry.data));
/// }
/// ```
///
/// # Panics
///
/// Panics if fsync fails with an I/O error to prevent silent data loss.
pub struct StorageEngine {
    dir: PathBuf,
    wal: Wal,
    inner: Arc<Mutex<Inner>>,
    config: Config,
    compaction: CompactionState,
    _lock_file: File,
}

impl StorageEngine {
    /// Opens or creates a storage engine at the given directory.
    ///
    /// If the directory contains existing data, it is recovered:
    /// - Incomplete writes from crashes are discarded
    /// - WAL entries not yet in `SSTables` are replayed
    /// - Orphaned temporary files are cleaned up
    ///
    /// The directory is locked to prevent concurrent access. Only one
    /// `StorageEngine` can open a directory at a time.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DirectoryLocked`] if another process holds the lock.
    /// Returns [`Error::Io`] if the directory cannot be created or accessed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::path::Path;
    /// # use lib_kifa::engine::{StorageEngine, Config};
    /// let (engine, report) = StorageEngine::open(Path::new("/data/logs"), Config::default())?;
    ///
    /// println!("Recovered {} entries", report.wal_entries_replayed);
    /// # Ok::<(), lib_kifa::engine::Error>(())
    /// ```
    pub fn open(dir: &Path, config: Config) -> Result<(Self, RecoveryReport), Error> {
        fs::create_dir_all(dir)?;

        let lock_file = acquire_lock(dir)?;

        let temp_sstables_cleaned = sstable::cleanup_temp_files(dir)?;
        let temp_manifests_cleaned = manifest::cleanup_temp_manifests(dir)?;
        let temp_files_cleaned = temp_sstables_cleaned + temp_manifests_cleaned;

        let manifest_path = manifest::manifest_path(dir);
        let manifest = if manifest_path.exists() {
            Manifest::load(&manifest_path, true)?
        } else {
            Manifest::new(manifest_path)
        };

        let orphan_sstables_cleaned = cleanup_orphan_sstables(dir, &manifest)?;

        let wal = Wal::open(dir)?;

        let checkpoint_lsn = manifest.checkpoint_lsn();
        let (memtable, replay_report) = replay_wal_from_checkpoint(dir, checkpoint_lsn)?;

        let trigger = Arc::new((Mutex::new(false), Condvar::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let emergency = Arc::new(AtomicBool::new(false));

        let inner = Arc::new(Mutex::new(Inner { memtable, manifest }));

        let compaction_handle = if config.compaction_enabled {
            Some(spawn_compaction_thread(
                dir.to_path_buf(),
                Arc::clone(&inner),
                Arc::clone(&trigger),
                Arc::clone(&shutdown),
                Arc::clone(&emergency),
                config.compaction_threshold,
            ))
        } else {
            None
        };

        let engine = Self {
            dir: dir.to_path_buf(),
            wal,
            inner,
            config,
            compaction: CompactionState { handle: compaction_handle, trigger, shutdown, emergency },
            _lock_file: lock_file,
        };

        let report = {
            let inner = engine.inner.lock().unwrap_or_else(sync::PoisonError::into_inner);
            RecoveryReport {
                wal_entries_replayed: replay_report.entries_replayed,
                checkpoint_lsn,
                first_replayed_lsn: replay_report.first_lsn,
                last_replayed_lsn: replay_report.last_lsn,
                first_timestamp_ms: replay_report.first_timestamp_ms,
                last_timestamp_ms: replay_report.last_timestamp_ms,
                gaps: replay_report.gaps,
                sstable_count: inner.manifest.sstable_count(),
                temp_files_cleaned,
                orphan_sstables_cleaned,
            }
        };

        Ok((engine, report))
    }

    /// Appends data to the log and returns its sequence number.
    ///
    /// The entry is written to the WAL and added to the memtable. Durability
    /// depends on the current [`FlushMode`]:
    ///
    /// - **Cautious/Emergency**: Returns after fsync — data survives power loss
    /// - **Normal**: Returns after write — fsync happens within 50 writes
    ///
    /// # Arguments
    ///
    /// * `data` - Arbitrary bytes to store (max 1 MiB per entry)
    ///
    /// # Returns
    ///
    /// The log sequence number (LSN) assigned to this entry. LSNs are
    /// monotonically increasing and unique.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails. The engine remains in a
    /// consistent state and can accept more writes.
    ///
    /// # Panics
    ///
    /// Panics if fsync fails to prevent silent data loss.
    pub fn append(&self, data: &[u8]) -> Result<u64, Error> {
        let mut inner = self.inner.lock().unwrap_or_else(sync::PoisonError::into_inner);

        let (lsn, timestamp_ms) = self.wal.append(data)?;
        inner.memtable.insert(lsn, timestamp_ms, Arc::from(data));

        if inner.memtable.size_bytes() >= self.config.memtable_flush_threshold {
            // Releases the lock before flushing to avoid deadlock.
            drop(inner);
            self.flush_internal()?;
        }

        Ok(lsn)
    }

    /// Forces the memtable to be flushed to an `SSTable`.
    ///
    /// Normally, flushing happens automatically when the memtable reaches
    /// [`Config::memtable_flush_threshold`]. Call this manually to:
    /// - Ensure data is in `SSTable` format (not just WAL)
    /// - Free memory before a large batch
    /// - Prepare for clean shutdown
    ///
    /// # Returns
    ///
    /// `Some(info)` with details about the created `SSTable`, or `None` if
    /// the memtable was empty.
    ///
    /// # Errors
    ///
    /// Returns an error if `SSTable` creation or manifest update fails.
    /// The memtable is preserved on error.
    pub fn flush(&self) -> Result<Option<SstableInfo>, Error> {
        self.flush_internal()
    }

    /// Sets the durability mode for subsequent writes.
    ///
    /// See [`FlushMode`] for details on each mode. Changes take effect
    /// immediately for new writes.
    ///
    /// Setting [`FlushMode::Emergency`] also:
    /// - Immediately flushes the memtable
    /// - Pauses background compaction
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::path::Path;
    /// # use lib_kifa::engine::{StorageEngine, Config};
    /// # use lib_kifa::FlushMode;
    /// # let (engine, _) = StorageEngine::open(Path::new("/data"), Config::default()).unwrap();
    /// // Cautious mode syncs every write during normal operation.
    /// engine.set_flush_mode(FlushMode::Cautious);
    ///
    /// // Emergency mode flushes immediately when the UPS signals power loss.
    /// engine.set_flush_mode(FlushMode::Emergency);
    /// ```
    pub fn set_flush_mode(&self, mode: FlushMode) {
        self.wal.set_flush_mode(mode);

        if matches!(mode, FlushMode::Emergency) {
            log::warn!("Flush mode set to Emergency: compaction paused, memtable flushing");
            self.compaction.emergency.store(true, Ordering::Release);
            let _ = self.flush_internal();
        } else {
            log::info!("Flush mode set to {mode:?}");
            self.compaction.emergency.store(false, Ordering::Release);
            self.notify_compaction();
        }
    }

    /// Returns current statistics about the storage engine.
    ///
    /// Useful for monitoring memory usage, write throughput, and compaction
    /// progress. Values are point-in-time snapshots.
    #[must_use]
    pub fn stats(&self) -> Stats {
        let inner = self.inner.lock().unwrap_or_else(sync::PoisonError::into_inner);
        Stats {
            wal: self.wal.stats(),
            memtable_size_bytes: inner.memtable.size_bytes(),
            memtable_entry_count: inner.memtable.len(),
            sstable_count: inner.manifest.sstable_count(),
            checkpoint_lsn: inner.manifest.checkpoint_lsn(),
        }
    }

    /// Returns the number of `SSTables` on disk.
    #[must_use]
    pub fn sstable_count(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(sync::PoisonError::into_inner);
        inner.manifest.sstable_count()
    }

    fn flush_internal(&self) -> Result<Option<SstableInfo>, Error> {
        let mut inner = self.inner.lock().unwrap_or_else(sync::PoisonError::into_inner);

        if inner.memtable.is_empty() {
            return Ok(None);
        }

        // Freezing prevents new inserts while the memtable is being flushed to disk.
        inner.memtable.freeze();

        let min_lsn = inner.memtable.iter().next().map_or(0, |e| e.lsn);
        let max_lsn = inner.memtable.last_lsn();
        let sstable_path = self.dir.join(sstable::sstable_name(min_lsn, max_lsn));

        let info = match sstable::flush_memtable(&inner.memtable, &sstable_path) {
            Ok(info) => info,
            Err(e) => {
                inner.memtable.unfreeze();
                return Err(e.into());
            }
        };

        let manifest_result =
            inner.manifest.register_sstable(info.path.clone(), info.min_lsn, info.max_lsn);

        if let Err(e) = manifest_result {
            let _ = fs::remove_file(&sstable_path);
            inner.memtable.unfreeze();
            return Err(e.into());
        }

        if let Err(e) = inner.manifest.save() {
            let _ = fs::remove_file(&sstable_path);
            inner.memtable.unfreeze();
            return Err(e.into());
        }

        // Entries up to checkpoint_lsn are now durable in SSTables and no longer need WAL replay.
        let checkpoint_lsn = inner.manifest.checkpoint_lsn();
        let _ = self.wal.truncate_log(checkpoint_lsn);

        inner.memtable = Memtable::new();

        log::info!(
            "Memtable flushed: {} entries, LSN {}-{}, path {}",
            info.entry_count,
            info.min_lsn,
            info.max_lsn,
            info.path.display()
        );

        self.notify_compaction();

        Ok(Some(info))
    }

    fn notify_compaction(&self) {
        let (lock, cvar) = &*self.compaction.trigger;
        if let Ok(mut triggered) = lock.lock() {
            *triggered = true;
            cvar.notify_one();
        }
    }

    /// Retrieves an entry by its log sequence number.
    ///
    /// Searches the memtable first, then `SSTables`. Returns `None` if no
    /// entry exists with the given LSN.
    ///
    /// # Errors
    ///
    /// Returns an error if `SSTable` reading fails.
    pub fn get(&self, lsn: u64) -> Result<Option<Entry>, Error> {
        let snapshot = self.snapshot();
        snapshot.get(lsn)
    }

    /// Returns entries within an LSN range (inclusive).
    ///
    /// Entries are returned in LSN order. Efficient for range queries
    /// across both memtable and `SSTables`.
    ///
    /// # Arguments
    ///
    /// * `start_lsn` - First LSN to include (inclusive)
    /// * `end_lsn` - Last LSN to include (inclusive)
    ///
    /// # Errors
    ///
    /// Returns an error if `SSTable` reading fails.
    pub fn scan(&self, start_lsn: u64, end_lsn: u64) -> Result<Vec<Entry>, Error> {
        let snapshot = self.snapshot();
        snapshot.scan(start_lsn, end_lsn)
    }

    /// Returns an iterator over all entries in LSN order.
    ///
    /// Merges entries from the memtable and all `SSTables`. Use this for
    /// full scans or exports.
    ///
    /// # Errors
    ///
    /// Returns an error if `SSTable` files cannot be opened.
    pub fn entries(&self) -> Result<MergeIter, Error> {
        let snapshot = self.snapshot();
        snapshot.merge_iter()
    }

    /// Creates a point-in-time snapshot for reading.
    ///
    /// The snapshot sees all entries that existed when it was created.
    /// New writes after snapshot creation are not visible to the snapshot.
    /// This allows concurrent reads without blocking writes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::path::Path;
    /// # use lib_kifa::engine::{StorageEngine, Config};
    /// # let (engine, _) = StorageEngine::open(Path::new("/data"), Config::default()).unwrap();
    /// let snapshot = engine.snapshot();
    ///
    /// // The snapshot captures state at creation time; later writes are invisible.
    /// engine.append(b"new data").unwrap();
    ///
    /// let entries = snapshot.scan(1, u64::MAX).unwrap();
    /// for entry in &entries {
    ///     println!("[{}] {:?}", entry.lsn, entry.data);
    /// }
    /// ```
    #[must_use]
    pub fn snapshot(&self) -> ReadSnapshot {
        let inner = self.inner.lock().unwrap_or_else(sync::PoisonError::into_inner);

        let memtable_entries: Vec<_> = inner
            .memtable
            .iter()
            .map(|e| Entry { lsn: e.lsn, timestamp_ms: e.timestamp_ms, data: e.data.clone() })
            .collect();

        let sstable_entries = inner.manifest.sstables().to_vec();

        ReadSnapshot { memtable_entries: Arc::new(memtable_entries), sstable_entries }
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        self.compaction.shutdown.store(true, Ordering::Release);
        self.notify_compaction();
        if let Some(handle) = self.compaction.handle.take() {
            let _ = handle.join();
        }
    }
}

/// A point-in-time view of the storage engine for reading.
///
/// Snapshots provide isolation from concurrent writes. Create a snapshot
/// via [`StorageEngine::snapshot`], then read from it without blocking
/// ongoing writes.
pub struct ReadSnapshot {
    memtable_entries: Arc<Vec<Entry>>,
    sstable_entries: Vec<SstableEntry>,
}

impl ReadSnapshot {
    /// Retrieves an entry by its log sequence number.
    ///
    /// Returns `None` if no entry with the given LSN exists in this snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if `SSTable` reading fails.
    pub fn get(&self, lsn: u64) -> Result<Option<Entry>, Error> {
        for entry in self.memtable_entries.iter() {
            if entry.lsn == lsn {
                return Ok(Some(Entry {
                    lsn: entry.lsn,
                    timestamp_ms: entry.timestamp_ms,
                    data: entry.data.clone(),
                }));
            }
        }

        for sstable in &self.sstable_entries {
            if lsn < sstable.min_lsn || lsn > sstable.max_lsn {
                continue;
            }

            let reader = SstableReader::open(&sstable.path)?;
            let mut iter = reader.into_iter();
            for entry in iter.by_ref() {
                if entry.lsn == lsn {
                    return Ok(Some(entry));
                }
            }

            if let Some(e) = iter.take_error() {
                return Err(e.into());
            }
        }

        Ok(None)
    }

    /// Returns entries within an LSN range (inclusive).
    ///
    /// # Errors
    ///
    /// Returns an error if `SSTable` reading fails.
    pub fn scan(&self, start_lsn: u64, end_lsn: u64) -> Result<Vec<Entry>, Error> {
        let mut result = Vec::new();

        let mut iter =
            MergeIter::new(Arc::clone(&self.memtable_entries), self.sstable_entries.clone())?;
        for entry in iter.by_ref() {
            if entry.lsn < start_lsn {
                continue;
            }
            if entry.lsn > end_lsn {
                break;
            }
            result.push(entry);
        }

        if let Some(e) = iter.take_error() {
            return Err(e);
        }

        Ok(result)
    }

    /// Returns an iterator over all entries in LSN order.
    ///
    /// # Errors
    ///
    /// Returns an error if `SSTable` files cannot be opened.
    pub fn merge_iter(&self) -> Result<MergeIter, Error> {
        MergeIter::new(Arc::clone(&self.memtable_entries), self.sstable_entries.clone())
    }
}

enum Source {
    Memtable { entries: Arc<Vec<Entry>>, pos: usize },
    Sstable(SstableIter),
}

/// Iterator that merges entries from memtable and `SSTables` in LSN order.
///
/// Uses a min-heap to efficiently merge multiple sorted sources. If an
/// I/O error occurs during iteration, the iterator stops and the error
/// can be retrieved via [`take_error`](Self::take_error).
pub struct MergeIter {
    heap: BinaryHeap<Reverse<HeapEntry>>,
    sources: Vec<Source>,
    error: Option<Error>,
}

impl MergeIter {
    fn new(
        memtable_entries: Arc<Vec<Entry>>,
        sstable_entries: Vec<SstableEntry>,
    ) -> Result<Self, Error> {
        let mut sources = Vec::with_capacity(1 + sstable_entries.len());
        let mut heap = BinaryHeap::new();

        if !memtable_entries.is_empty() {
            let first = &memtable_entries[0];
            heap.push(Reverse(HeapEntry {
                lsn: first.lsn,
                timestamp_ms: first.timestamp_ms,
                data: first.data.clone(),
                source_idx: sources.len(),
            }));
            sources.push(Source::Memtable { entries: memtable_entries, pos: 1 });
        }

        for sstable in sstable_entries {
            let reader = SstableReader::open(&sstable.path)?;
            let mut iter = reader.into_iter();

            if let Some(entry) = iter.next() {
                let source_idx = sources.len();
                heap.push(Reverse(HeapEntry {
                    lsn: entry.lsn,
                    timestamp_ms: entry.timestamp_ms,
                    data: entry.data,
                    source_idx,
                }));
                sources.push(Source::Sstable(iter));
            }
        }

        Ok(Self { heap, sources, error: None })
    }

    /// Takes the error that stopped iteration, if any.
    ///
    /// Call this after iteration completes to check if it stopped due to
    /// an I/O error rather than reaching the end.
    #[must_use]
    pub const fn take_error(&mut self) -> Option<Error> {
        self.error.take()
    }

    fn advance_source(&mut self, source_idx: usize) {
        let next_entry = match &mut self.sources[source_idx] {
            Source::Memtable { entries, pos } => {
                let entry = entries.get(*pos).map(|e| HeapEntry {
                    lsn: e.lsn,
                    timestamp_ms: e.timestamp_ms,
                    data: e.data.clone(),
                    source_idx,
                });
                *pos += 1;
                entry
            }
            Source::Sstable(iter) => {
                if let Some(e) = iter.next() {
                    Some(HeapEntry {
                        lsn: e.lsn,
                        timestamp_ms: e.timestamp_ms,
                        data: e.data,
                        source_idx,
                    })
                } else {
                    if let Some(e) = iter.take_error() {
                        self.error = Some(e.into());
                    }
                    None
                }
            }
        };

        if let Some(entry) = next_entry {
            self.heap.push(Reverse(entry));
        }
    }
}

impl Iterator for MergeIter {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() {
            return None;
        }

        let Reverse(entry) = self.heap.pop()?;

        self.advance_source(entry.source_idx);

        Some(Entry { lsn: entry.lsn, timestamp_ms: entry.timestamp_ms, data: entry.data })
    }
}

fn spawn_compaction_thread(
    dir: PathBuf,
    inner: Arc<Mutex<Inner>>,
    trigger: Arc<(Mutex<bool>, Condvar)>,
    shutdown: Arc<AtomicBool>,
    emergency: Arc<AtomicBool>,
    threshold: usize,
) -> JoinHandle<()> {
    thread::spawn(move || {
        compaction_loop(&dir, &inner, &trigger, &shutdown, &emergency, threshold);
    })
}

fn compaction_loop(
    dir: &Path,
    inner: &Arc<Mutex<Inner>>,
    trigger: &(Mutex<bool>, Condvar),
    shutdown: &AtomicBool,
    emergency: &AtomicBool,
    threshold: usize,
) {
    let (lock, cvar) = trigger;

    loop {
        // Checks shutdown before blocking to avoid a stale wait when the engine drops quickly.
        if shutdown.load(Ordering::Acquire) {
            break;
        }

        let mut triggered = lock.lock().unwrap_or_else(sync::PoisonError::into_inner);
        let result = cvar.wait_timeout(triggered, COMPACTION_POLL_INTERVAL);
        triggered = result.unwrap_or_else(sync::PoisonError::into_inner).0;
        // Resets and releases the lock so new triggers can arrive while compaction runs.
        *triggered = false;
        drop(triggered);

        if shutdown.load(Ordering::Acquire) {
            break;
        }

        if emergency.load(Ordering::Acquire) {
            log::warn!("Compaction paused: emergency mode active");
            continue;
        }

        let inputs = {
            let guard = inner.lock().unwrap_or_else(sync::PoisonError::into_inner);
            prepare_compaction(guard.manifest.sstables(), threshold)
        };

        let Some(inputs) = inputs else {
            continue;
        };

        let output = match run_compaction(dir, &inputs) {
            Ok(output) => output,
            Err(e) => {
                log::error!("Compaction failed: {e}");
                continue;
            }
        };

        // Rechecks shutdown after the potentially long-running compaction completes.
        if shutdown.load(Ordering::Acquire) {
            break;
        }

        let mut guard = inner.lock().unwrap_or_else(sync::PoisonError::into_inner);
        if let Ok(result) = commit_compaction(&mut guard.manifest, output) {
            log::info!(
                "Compacted {} SSTables into {}: {} entries, LSN {}-{}, removed {} files",
                result.input_count,
                result.output_path.display(),
                result.entry_count,
                result.min_lsn,
                result.max_lsn,
                result.removed_paths.len()
            );
        }
    }
}

fn acquire_lock(dir: &Path) -> Result<File, Error> {
    let lock_path = dir.join(LOCK_FILE_NAME);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&lock_path)
        .map_err(Error::Io)?;

    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(fs::TryLockError::WouldBlock) => Err(Error::DirectoryLocked),
        Err(fs::TryLockError::Error(e)) => Err(Error::Io(e)),
    }
}

fn cleanup_orphan_sstables(dir: &Path, manifest: &Manifest) -> Result<usize, Error> {
    let registered_names: Vec<_> = manifest
        .sstables()
        .iter()
        .filter_map(|e| e.path.file_name().map(OsStr::to_owned))
        .collect();
    let mut cleaned = 0;

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();

        if path.extension().and_then(|s| s.to_str()).is_some_and(|ext| ext == "sst")
            && path.file_name().is_none_or(|name| !registered_names.contains(&name.to_owned()))
        {
            fs::remove_file(&path)?;
            cleaned += 1;
        }
    }

    Ok(cleaned)
}

struct ReplayReport {
    entries_replayed: usize,
    first_lsn: Option<u64>,
    last_lsn: Option<u64>,
    first_timestamp_ms: Option<u64>,
    last_timestamp_ms: Option<u64>,
    gaps: Vec<Range<u64>>,
}

fn replay_wal_from_checkpoint(
    dir: &Path,
    checkpoint_lsn: u64,
) -> Result<(Memtable, ReplayReport), Error> {
    let mut memtable = Memtable::new();
    let mut entries_replayed = 0;
    let mut first_lsn = None;
    let mut last_lsn = None;
    let mut first_timestamp_ms = None;
    let mut last_timestamp_ms = None;
    let mut gaps = Vec::new();
    let mut expected_lsn = checkpoint_lsn + 1;

    for entry in Wal::entries(dir)? {
        // Entries at or below checkpoint_lsn are already persisted in SSTables.
        if entry.lsn <= checkpoint_lsn {
            continue;
        }

        if entry.lsn > expected_lsn {
            gaps.push(expected_lsn..entry.lsn);
        }

        if first_lsn.is_none() {
            first_lsn = Some(entry.lsn);
            first_timestamp_ms = Some(entry.timestamp_ms);
        }
        last_lsn = Some(entry.lsn);
        last_timestamp_ms = Some(entry.timestamp_ms);

        memtable.insert(entry.lsn, entry.timestamp_ms, entry.data);
        entries_replayed += 1;
        expected_lsn = entry.lsn + 1;
    }

    Ok((
        memtable,
        ReplayReport {
            entries_replayed,
            first_lsn,
            last_lsn,
            first_timestamp_ms,
            last_timestamp_ms,
            gaps,
        },
    ))
}
