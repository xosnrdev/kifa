//! Segmented write-ahead log for crash-proof persistence.
//!
//! Entries are written to 16MiB segment files and validated with dual CRCs to detect corruption.
//! On Linux, direct I/O bypasses the page cache so writes reach stable storage immediately.
//! Callers can adjust [`FlushMode`] to trade throughput for stronger durability guarantees.

use std::fs::{self, File, OpenOptions, read_dir};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use std::os::unix::{fs::OpenOptionsExt, io::AsRawFd};
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{self, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fmt, io};

use crate::buffer::AlignedBuffer;
use crate::helpers::{SECTOR_SIZE, sync_dir_path, sync_file};
use crate::{FlushMode, MEBI, map_err};

const SEGMENT_SIZE: usize = 16 * MEBI;
const MAX_ENTRY_SIZE: usize = MEBI;
const MAGIC_TRAILER: u64 = 0x1405_7B7E_F767_814F;
const ENTRY_HEADER_SIZE: usize = size_of::<EntryHeader>();
const ENTRY_FOOTER_SIZE: usize = size_of::<EntryFooter>();
const LSN_SIZE: usize = size_of::<u64>();
const TIMESTAMP_SIZE: usize = size_of::<u64>();
const ENTRY_OVERHEAD: usize = ENTRY_HEADER_SIZE + LSN_SIZE + TIMESTAMP_SIZE + ENTRY_FOOTER_SIZE;

const _: () = {
    assert!(ENTRY_HEADER_SIZE == 8);
    assert!(ENTRY_FOOTER_SIZE == 16);
    assert!(LSN_SIZE == 8);
    assert!(TIMESTAMP_SIZE == 8);
    assert!(ENTRY_OVERHEAD == 40);
    assert!(SEGMENT_SIZE.is_multiple_of(SECTOR_SIZE));
};

#[derive(Debug)]
pub enum Error {
    EntryTooLarge { size: usize, max: usize },
    SegmentFull { available: usize, required: usize },
    Io(io::Error),
    PreallocationFailed(io::Error),
    SegmentCreation(io::Error),
}

map_err!(Io, io::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EntryTooLarge { size, max } => {
                write!(f, "entry too large: {} MiB (max {} MiB)", size / MEBI, max / MEBI)
            }
            Self::SegmentFull { available, required } => {
                write!(
                    f,
                    "segment full: {} MiB available, {} MiB required",
                    available / MEBI,
                    required / MEBI
                )
            }
            Self::Io(e) => write!(f, "{e}"),
            Self::PreallocationFailed(e) => write!(f, "preallocation failed: {e}"),
            Self::SegmentCreation(e) => write!(f, "segment creation failed: {e}"),
        }
    }
}

impl std::error::Error for Error {}

#[repr(C)]
#[derive(Clone, Copy)]
struct EntryHeader {
    length: u32,
    header_crc: u32,
}

impl EntryHeader {
    fn new(data_len: usize) -> Self {
        let length = (LSN_SIZE + TIMESTAMP_SIZE + data_len + ENTRY_FOOTER_SIZE) as u32;
        let mut header = Self { length, header_crc: 0 };
        header.header_crc = header.compute_crc();
        header
    }

    fn compute_crc(self) -> u32 {
        crc32fast::hash(&self.length.to_le_bytes())
    }

    fn validate(self) -> bool {
        let expected = crc32fast::hash(&self.length.to_le_bytes());
        self.header_crc == expected
    }

    fn as_bytes(self) -> [u8; ENTRY_HEADER_SIZE] {
        let mut buf = [0; ENTRY_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.length.to_le_bytes());
        buf[4..8].copy_from_slice(&self.header_crc.to_le_bytes());
        buf
    }

    const fn from_bytes(bytes: [u8; ENTRY_HEADER_SIZE]) -> Self {
        Self {
            length: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            header_crc: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct EntryFooter {
    data_crc: u32,
    padding: u32,
    magic: u64,
}

impl EntryFooter {
    fn new(lsn: u64, timestamp_ms: u64, data: &[u8]) -> Self {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&lsn.to_le_bytes());
        hasher.update(&timestamp_ms.to_le_bytes());
        hasher.update(data);
        Self { data_crc: hasher.finalize(), padding: 0, magic: MAGIC_TRAILER }
    }

    fn validate(&self, lsn: u64, timestamp_ms: u64, data: &[u8]) -> bool {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&lsn.to_le_bytes());
        hasher.update(&timestamp_ms.to_le_bytes());
        hasher.update(data);
        self.magic == MAGIC_TRAILER && self.data_crc == hasher.finalize()
    }

    fn as_bytes(self) -> [u8; ENTRY_FOOTER_SIZE] {
        let mut buf = [0; ENTRY_FOOTER_SIZE];
        buf[0..4].copy_from_slice(&self.data_crc.to_le_bytes());
        buf[4..8].copy_from_slice(&self.padding.to_le_bytes());
        buf[8..16].copy_from_slice(&self.magic.to_le_bytes());
        buf
    }

    const fn from_bytes(bytes: [u8; ENTRY_FOOTER_SIZE]) -> Self {
        Self {
            data_crc: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            padding: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            magic: u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ]),
        }
    }
}

/// Statistics about the write-ahead log.
#[derive(Debug, Clone, Copy)]
pub struct WalStats {
    /// The LSN that will be assigned to the next appended entry.
    pub next_lsn: u64,

    /// The highest LSN that has been fsync'd to disk.
    ///
    /// Entries with LSN <= this value are guaranteed durable.
    pub last_durable_lsn: u64,

    /// Current flush mode.
    pub flush_mode: FlushMode,
}

#[cfg(unix)]
fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.seek_read(buf, offset)
}

#[cfg(unix)]
fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    file.write_at(buf, offset)
}

#[cfg(windows)]
fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    file.seek_write(buf, offset)
}

#[cfg(target_os = "linux")]
fn open_segment_file(path: &Path, create: bool) -> Result<File, Error> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true);

    opts.create_new(create);

    opts.custom_flags(libc::O_DIRECT | libc::O_DSYNC);

    opts.open(path).map_err(Error::SegmentCreation)
}

#[cfg(not(target_os = "linux"))]
fn open_segment_file(path: &Path, create: bool) -> Result<File, Error> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true);

    opts.create_new(create);

    opts.open(path).map_err(Error::SegmentCreation)
}

#[cfg(target_os = "linux")]
fn preallocate_segment(file: &File) -> Result<(), Error> {
    // SAFETY: `as_raw_fd()` returns a valid file descriptor owned by `file`. The offset (0) and
    // length (`SEGMENT_SIZE`) are within valid ranges. The file descriptor remains valid for the
    // duration of this call because we hold a reference to `file`.
    let result = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_ZERO_RANGE,
            0,
            SEGMENT_SIZE as libc::off_t,
        )
    };

    if result != 0 {
        return Err(Error::PreallocationFailed(io::Error::last_os_error()));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn preallocate_segment(file: &File) -> Result<(), Error> {
    file.set_len(SEGMENT_SIZE as u64).map_err(Error::PreallocationFailed)
}

fn discover_segments(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut segments = Vec::new();

    for entry in read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()).is_some_and(|s| s == "wal") {
            segments.push(path);
        }
    }

    segments.sort();
    Ok(segments)
}

fn segment_name(sequence: u64) -> String {
    format!("{sequence:016x}.wal")
}

pub struct RecoveredEntry {
    pub lsn: u64,
    pub timestamp_ms: u64,
    pub data: Vec<u8>,
}

fn scan_segment(path: &Path) -> Result<(Vec<RecoveredEntry>, u64), Error> {
    let file = open_segment_file(path, false)?;
    let mut entries = Vec::new();
    let mut offset = 0;

    // pread(2) reads into buf before returning. Uninitialized memory is
    // sound because the caller only accesses bytes within bytes_read.
    let mut header_buf = AlignedBuffer::new_uninit(SECTOR_SIZE);

    loop {
        if offset + ENTRY_OVERHEAD as u64 > SEGMENT_SIZE as u64 {
            break;
        }

        let Ok(bytes_read) = pread(&file, header_buf.as_mut_slice(), offset) else {
            break;
        };

        // pread(2): "it is not an error for a successful call to transfer
        // fewer bytes than requested." O_DIRECT on Linux transfers complete
        // sectors, but other platforms lack this guarantee.
        if bytes_read < ENTRY_HEADER_SIZE {
            break;
        }

        let header =
            EntryHeader::from_bytes(header_buf.as_slice()[..ENTRY_HEADER_SIZE].try_into().unwrap());

        if !header.validate() {
            break;
        }

        if header.length == 0 {
            break;
        }

        let payload_len = header.length as usize;
        let total_entry_size = ENTRY_HEADER_SIZE + payload_len;
        let aligned_entry_size = total_entry_size.next_multiple_of(SECTOR_SIZE);

        if offset + aligned_entry_size as u64 > SEGMENT_SIZE as u64 {
            break;
        }

        // pread(2) reads into buf before returning. Uninitialized memory is
        // sound because the caller only accesses bytes within bytes_read.
        let mut entry_buf = AlignedBuffer::new_uninit(aligned_entry_size);
        let Ok(bytes_read) = pread(&file, entry_buf.as_mut_slice(), offset) else {
            break;
        };

        if bytes_read < aligned_entry_size {
            break;
        }

        let lsn_start = ENTRY_HEADER_SIZE;
        let lsn_end = lsn_start + LSN_SIZE;
        let lsn = u64::from_le_bytes(entry_buf.as_slice()[lsn_start..lsn_end].try_into().unwrap());

        let timestamp_start = lsn_end;
        let timestamp_end = timestamp_start + TIMESTAMP_SIZE;
        let timestamp_ms = u64::from_le_bytes(
            entry_buf.as_slice()[timestamp_start..timestamp_end].try_into().unwrap(),
        );

        let data_start = timestamp_end;
        let data_len = payload_len - LSN_SIZE - TIMESTAMP_SIZE - ENTRY_FOOTER_SIZE;
        let data_end = data_start + data_len;
        let data = entry_buf.as_slice()[data_start..data_end].to_vec();

        let footer_start = data_end;
        let footer_end = footer_start + ENTRY_FOOTER_SIZE;
        let footer = EntryFooter::from_bytes(
            entry_buf.as_slice()[footer_start..footer_end].try_into().unwrap(),
        );

        if !footer.validate(lsn, timestamp_ms, &data) {
            break;
        }

        entries.push(RecoveredEntry { lsn, timestamp_ms, data });

        offset += aligned_entry_size as u64;
    }

    Ok((entries, offset))
}

struct SegmentWriter {
    file: File,
    offset: u64,
    sequence: u64,
}

impl SegmentWriter {
    fn new(dir: &Path, sequence: u64) -> Result<Self, Error> {
        let path = dir.join(segment_name(sequence));
        let file = open_segment_file(&path, true)?;
        preallocate_segment(&file)?;
        sync_dir_path(dir)?;

        Ok(Self { file, offset: 0, sequence })
    }

    fn open_existing(path: &Path, sequence: u64, offset: u64) -> Result<Self, Error> {
        let file = open_segment_file(path, false)?;

        Ok(Self { file, offset, sequence })
    }

    const fn remaining_space(&self) -> usize {
        SEGMENT_SIZE.saturating_sub(self.offset as usize)
    }

    fn write_entry(&mut self, lsn: u64, timestamp_ms: u64, data: &[u8]) -> Result<(), Error> {
        let header = EntryHeader::new(data.len());
        let footer = EntryFooter::new(lsn, timestamp_ms, data);

        let total_size =
            ENTRY_HEADER_SIZE + LSN_SIZE + TIMESTAMP_SIZE + data.len() + ENTRY_FOOTER_SIZE;
        let aligned_size = total_size.next_multiple_of(SECTOR_SIZE);

        if aligned_size > self.remaining_space() {
            return Err(Error::SegmentFull {
                available: self.remaining_space(),
                required: aligned_size,
            });
        }

        // The buffer starts uninitialized. All content bytes are written below, and
        // padding bytes are explicitly zeroed after. The header, LSN, timestamp, data,
        // and footer regions do not need pre-zeroing since they are overwritten.
        let mut buffer = AlignedBuffer::new_uninit(aligned_size);
        let buf = buffer.as_mut_slice();

        let mut pos = 0;
        buf[pos..pos + ENTRY_HEADER_SIZE].copy_from_slice(&header.as_bytes());
        pos += ENTRY_HEADER_SIZE;

        buf[pos..pos + LSN_SIZE].copy_from_slice(&lsn.to_le_bytes());
        pos += LSN_SIZE;

        buf[pos..pos + TIMESTAMP_SIZE].copy_from_slice(&timestamp_ms.to_le_bytes());
        pos += TIMESTAMP_SIZE;

        buf[pos..pos + data.len()].copy_from_slice(data);
        pos += data.len();

        buf[pos..pos + ENTRY_FOOTER_SIZE].copy_from_slice(&footer.as_bytes());
        pos += ENTRY_FOOTER_SIZE;

        // Sector alignment leaves trailing bytes unwritten. Zeroing them stops stale
        // memory contents from reaching disk and keeps WAL content deterministic.
        buf[pos..].fill(0);

        let bytes_written = pwrite(&self.file, &buf[..aligned_size], self.offset)?;

        if bytes_written < aligned_size {
            return Err(Error::Io(io::Error::new(io::ErrorKind::WriteZero, "incomplete write")));
        }

        self.offset += aligned_size as u64;
        Ok(())
    }

    fn sync(&self) {
        sync_file(&self.file);
    }
}

pub struct Wal {
    dir: PathBuf,
    writer: Mutex<SegmentWriter>,
    next_lsn: AtomicU64,
    flush_mode: Mutex<FlushMode>,
    pending_syncs: AtomicU64,
    last_durable_lsn: AtomicU64,
}

impl Wal {
    pub fn open(dir: &Path) -> Result<Self, Error> {
        fs::create_dir_all(dir)?;

        let segments = discover_segments(dir)?;
        let (next_lsn, writer) = if segments.is_empty() {
            let writer = SegmentWriter::new(dir, 0)?;
            (1, writer)
        } else {
            let mut max_lsn = 0;
            let mut last_segment_path = None;
            let mut last_segment_seq = 0;
            let mut last_segment_offset = 0;

            for path in &segments {
                let (entries, offset) = scan_segment(path)?;
                if let Some(entry) = entries.last()
                    && entry.lsn >= max_lsn
                {
                    max_lsn = entry.lsn;
                    last_segment_path = Some(path.clone());
                    last_segment_offset = offset;

                    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("0");
                    last_segment_seq = u64::from_str_radix(stem, 16).unwrap_or_default();
                }
            }

            let writer = if let Some(path) = last_segment_path {
                if last_segment_offset < SEGMENT_SIZE as u64 {
                    SegmentWriter::open_existing(&path, last_segment_seq, last_segment_offset)?
                } else {
                    SegmentWriter::new(dir, last_segment_seq + 1)?
                }
            } else {
                // Segment file exists but contains no valid entries.
                let first_segment = &segments[0];
                let stem = first_segment.file_stem().and_then(|s| s.to_str()).unwrap_or("0");
                let seq = u64::from_str_radix(stem, 16).unwrap_or_default();
                SegmentWriter::open_existing(first_segment, seq, 0)?
            };

            (max_lsn + 1, writer)
        };

        let last_durable = next_lsn.saturating_sub(1);

        Ok(Self {
            dir: dir.to_path_buf(),
            writer: Mutex::new(writer),
            next_lsn: AtomicU64::new(next_lsn),
            flush_mode: Mutex::new(FlushMode::Normal),
            pending_syncs: AtomicU64::new(0),
            last_durable_lsn: AtomicU64::new(last_durable),
        })
    }

    pub fn append(&self, data: &[u8]) -> Result<(u64, u64), Error> {
        if data.len() > MAX_ENTRY_SIZE {
            return Err(Error::EntryTooLarge { size: data.len(), max: MAX_ENTRY_SIZE });
        }

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let timestamp_ms =
            SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64);
        let mut writer = self.writer.lock().unwrap_or_else(sync::PoisonError::into_inner);

        let total_size = ENTRY_OVERHEAD + data.len();
        let aligned_size = total_size.next_multiple_of(SECTOR_SIZE);

        if aligned_size > writer.remaining_space() {
            let new_seq = writer.sequence + 1;
            log::debug!("WAL segment rotated: sequence {} -> {}", writer.sequence, new_seq);
            writer.sync();
            *writer = SegmentWriter::new(&self.dir, new_seq)?;
        }

        writer.write_entry(lsn, timestamp_ms, data)?;

        self.sync_for_mode(&writer);

        Ok((lsn, timestamp_ms))
    }

    #[cfg(target_os = "linux")]
    fn sync_for_mode(&self, writer: &SegmentWriter) {
        let mode = *self.flush_mode.lock().unwrap_or_else(sync::PoisonError::into_inner);
        match mode {
            FlushMode::Emergency | FlushMode::Cautious => writer.sync(),
            FlushMode::Normal => {
                if self.pending_syncs.fetch_add(1, Ordering::SeqCst) + 1 >= 50 {
                    writer.sync();
                    self.pending_syncs.store(0, Ordering::SeqCst);
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn sync_for_mode(&self, writer: &SegmentWriter) {
        writer.sync();
        self.pending_syncs.store(0, Ordering::SeqCst);
    }

    pub fn sync(&self) {
        let writer = self.writer.lock().unwrap_or_else(sync::PoisonError::into_inner);
        writer.sync();
        self.pending_syncs.store(0, Ordering::SeqCst);

        let current = self.next_lsn.load(Ordering::SeqCst);
        if current > 1 {
            self.last_durable_lsn.store(current - 1, Ordering::SeqCst);
        }
    }

    pub fn stats(&self) -> WalStats {
        let mode = *self.flush_mode.lock().unwrap_or_else(sync::PoisonError::into_inner);

        WalStats {
            next_lsn: self.next_lsn.load(Ordering::SeqCst),
            last_durable_lsn: self.last_durable_lsn.load(Ordering::SeqCst),
            flush_mode: mode,
        }
    }

    pub fn set_flush_mode(&self, mode: FlushMode) {
        *self.flush_mode.lock().unwrap_or_else(sync::PoisonError::into_inner) = mode;
        if mode == FlushMode::Emergency {
            self.sync();
        }
    }

    pub fn truncate_log(&self, checkpoint_lsn: u64) -> Result<usize, Error> {
        let segments = discover_segments(&self.dir)?;
        let mut deleted_count = 0;

        let writer = self.writer.lock().unwrap_or_else(sync::PoisonError::into_inner);
        let current_sequence = writer.sequence;
        drop(writer);

        for path in segments {
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("0");
            let sequence = u64::from_str_radix(stem, 16).unwrap_or_default();

            if sequence >= current_sequence {
                continue;
            }

            let (entries, _) = scan_segment(&path)?;
            let max_lsn_in_segment = entries.iter().map(|e| e.lsn).max().unwrap_or_default();

            if max_lsn_in_segment <= checkpoint_lsn {
                fs::remove_file(&path)?;
                deleted_count += 1;
            }
        }

        if deleted_count > 0 {
            sync_dir_path(&self.dir)?;
            log::debug!(
                "WAL truncated: {deleted_count} segments removed, checkpoint LSN {checkpoint_lsn}"
            );
        }

        Ok(deleted_count)
    }

    pub fn entries(dir: &Path) -> Result<impl Iterator<Item = RecoveredEntry>, Error> {
        let segments = discover_segments(dir)?;
        let mut all_entries = Vec::new();

        for path in segments {
            let (entries, _) = scan_segment(&path)?;
            all_entries.extend(entries);
        }

        Ok(all_entries.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    struct RecoveryReport {
        recovered_count: usize,
        first_lsn: Option<u64>,
        last_lsn: Option<u64>,
    }

    fn recover(dir: &Path) -> Result<RecoveryReport, Error> {
        let segments = discover_segments(dir)?;

        let mut recovered_count = 0;
        let mut first_lsn = None;
        let mut last_lsn = None;

        for path in segments {
            let (entries, _) = scan_segment(&path)?;
            recovered_count += entries.len();
            if let Some(entry) = entries.first()
                && first_lsn.is_none()
            {
                first_lsn = Some(entry.lsn);
            }
            if let Some(entry) = entries.last() {
                last_lsn = Some(entry.lsn);
            }
        }

        Ok(RecoveryReport { recovered_count, first_lsn, last_lsn })
    }

    #[test]
    fn test_entry_header_crc() {
        let header = EntryHeader::new(100);
        assert!(header.validate());

        let bytes = header.as_bytes();
        let restored = EntryHeader::from_bytes(bytes);
        assert!(restored.validate());
        assert_eq!(header.length, restored.length);
    }

    #[test]
    fn test_entry_footer_validation() {
        let lsn = 42;
        let timestamp_ms = 1000;
        let data = b"foo bar";
        let footer = EntryFooter::new(lsn, timestamp_ms, data);
        assert!(footer.validate(lsn, timestamp_ms, data));
        assert!(!footer.validate(lsn, timestamp_ms, b"bar foo"));
        assert!(!footer.validate(99, timestamp_ms, data));
        assert!(!footer.validate(lsn, 9999, data));
    }

    #[test]
    fn test_wal_reopen() {
        let dir = TempDir::new().unwrap();

        {
            let wal = Wal::open(dir.path()).unwrap();
            wal.append(b"foo").unwrap();
            wal.append(b"bar").unwrap();
            wal.sync();
        }

        {
            let wal = Wal::open(dir.path()).unwrap();
            let (lsn, _) = wal.append(b"baz").unwrap();
            assert_eq!(lsn, 3);
            wal.sync();
        }

        let report = recover(dir.path()).unwrap();
        assert_eq!(report.recovered_count, 3);
        assert_eq!(report.last_lsn, Some(3));
    }

    #[test]
    fn test_entry_too_large() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(dir.path()).unwrap();

        let large_data = vec![0; MAX_ENTRY_SIZE + 1];
        let result = wal.append(&large_data);

        assert!(matches!(result, Err(Error::EntryTooLarge { .. })));
    }

    #[test]
    fn test_flush_modes() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(dir.path()).unwrap();

        wal.set_flush_mode(FlushMode::Emergency);
        wal.append(b"foo").unwrap();

        wal.set_flush_mode(FlushMode::Cautious);
        wal.append(b"bar").unwrap();

        wal.set_flush_mode(FlushMode::Normal);
        wal.append(b"baz").unwrap();
        wal.sync();

        let report = recover(dir.path()).unwrap();
        assert_eq!(report.recovered_count, 3);
    }

    #[test]
    fn test_recovery_report_first_lsn() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(dir.path()).unwrap();

        wal.append(b"foo").unwrap();
        wal.append(b"bar").unwrap();
        wal.append(b"baz").unwrap();
        wal.sync();
        drop(wal);

        let report = recover(dir.path()).unwrap();
        assert_eq!(report.first_lsn, Some(1));
        assert_eq!(report.last_lsn, Some(3));
        assert_eq!(report.recovered_count, 3);
    }

    #[test]
    fn test_wal_stats_initial() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(dir.path()).unwrap();

        let stats = wal.stats();
        assert_eq!(stats.next_lsn, 1);
        assert_eq!(stats.last_durable_lsn, 0);
        assert_eq!(stats.flush_mode, FlushMode::Normal);
    }

    #[test]
    fn test_wal_stats_after_writes() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(dir.path()).unwrap();

        wal.append(b"foo").unwrap();
        wal.append(b"bar").unwrap();
        wal.sync();

        let stats = wal.stats();
        assert_eq!(stats.next_lsn, 3);
        assert_eq!(stats.last_durable_lsn, 2);
    }

    #[test]
    fn test_wal_stats_flush_mode() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(dir.path()).unwrap();

        wal.set_flush_mode(FlushMode::Emergency);
        let stats = wal.stats();
        assert_eq!(stats.flush_mode, FlushMode::Emergency);
    }

    #[test]
    fn test_wal_stats_after_reopen() {
        let dir = TempDir::new().unwrap();

        {
            let wal = Wal::open(dir.path()).unwrap();
            wal.append(b"foo").unwrap();
            wal.append(b"bar").unwrap();
            wal.sync();
        }

        {
            let wal = Wal::open(dir.path()).unwrap();
            let stats = wal.stats();
            assert_eq!(stats.next_lsn, 3);
            assert_eq!(stats.last_durable_lsn, 2);
        }
    }

    #[test]
    fn test_truncate_preserves_active_segment() {
        let dir = TempDir::new().unwrap();
        let wal = Wal::open(dir.path()).unwrap();

        wal.append(b"foo").unwrap();
        wal.append(b"bar").unwrap();
        wal.sync();

        assert_eq!(wal.truncate_log(0).unwrap(), 0, "No segments should be deleted");

        assert_eq!(wal.truncate_log(1000).unwrap(), 0, "Active segment should not be deleted");

        assert_eq!(discover_segments(dir.path()).unwrap().len(), 1);
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "Filling a 16MiB segment requires ~4096 fsyncs without O_DIRECT"
    )]
    fn test_truncate_deletes_old_segments() {
        let dir = TempDir::new().unwrap();

        {
            let wal = Wal::open(dir.path()).unwrap();
            let entry_size = SECTOR_SIZE - ENTRY_OVERHEAD;
            let data = vec![0; entry_size];
            let entries_per_segment = SEGMENT_SIZE / SECTOR_SIZE;

            for _ in 0..entries_per_segment {
                wal.append(&data).unwrap();
            }
            wal.sync();

            wal.append(b"foo").unwrap();
            wal.sync();
        }

        let segments_before = discover_segments(dir.path()).unwrap();
        assert_eq!(segments_before.len(), 2);

        {
            let wal = Wal::open(dir.path()).unwrap();

            let first_segment_max_lsn = (SEGMENT_SIZE / SECTOR_SIZE) as u64;
            let deleted = wal.truncate_log(first_segment_max_lsn).unwrap();
            assert_eq!(deleted, 1);
        }

        let segments_after = discover_segments(dir.path()).unwrap();
        assert_eq!(segments_after.len(), 1);
    }

    #[test]
    fn test_reopen_empty_segment() {
        let dir = TempDir::new().unwrap();

        {
            let _wal = Wal::open(dir.path()).unwrap();
        }

        let segments = discover_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 1, "There should be one segment file present");

        let wal = Wal::open(dir.path()).unwrap();

        let (lsn, _) = wal.append(b"after restart").unwrap();

        assert_eq!(lsn, 1, "LSN should start at 1 after reopening empty segment");
        wal.sync();

        let report = recover(dir.path()).unwrap();
        assert_eq!(report.recovered_count, 1);
        assert_eq!(report.first_lsn, Some(1));
    }
}
