//! Log-ordered `SSTable` for persisting memtable flushes.
//!
//! Unlike key-sorted `SSTables` in `LevelDB` or `RocksDB`, entries here are ordered by LSN.
//! This suits append-only log workloads where reads are sequential scans, not point lookups.
//! Writes go to a temp file first; an atomic rename commits the final `SSTable` to disk.

use std::fmt;
use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::common::{atomic_rename, temp_path};
use crate::helpers::{VERSION, sync_file};
use crate::memtable::{Entry, Memtable};
use crate::{MEBI, map_err};

pub const MAGIC_HEADER: u64 = 0x5851_F42D_4C95_7F2D;
const MAGIC_FOOTER: u64 = 0x27BB_2EE6_87B0_B0FD;
pub const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 16;
pub const MAX_ENTRY_SIZE: usize = MEBI;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidMagic,
    UnsupportedVersion { found: u32, expected: u32 },
    CorruptedData,
    EmptyMemtable,
    LsnNotMonotonic { current: u64, previous: u64 },
    TempFileCleanup(io::Error),
    EntryTooLarge { size: usize, max: usize },
}

map_err!(Io, io::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::InvalidMagic => write!(f, "invalid magic number"),
            Self::UnsupportedVersion { found, expected } => {
                write!(f, "unsupported version: {found} (expected {expected})")
            }
            Self::CorruptedData => write!(f, "corrupted data"),
            Self::EmptyMemtable => write!(f, "empty memtable"),
            Self::LsnNotMonotonic { current, previous } => {
                write!(f, "LSN not monotonic: {current} <= {previous}")
            }
            Self::TempFileCleanup(e) => write!(f, "temp file cleanup failed: {e}"),
            Self::EntryTooLarge { size, max } => {
                write!(f, "entry too large: {size} bytes (max {max})")
            }
        }
    }
}

impl std::error::Error for Error {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Header {
    pub magic: u64,
    pub version: u32,
    pub entry_count: u32,
    pub min_lsn: u64,
    pub max_lsn: u64,
}

impl Header {
    pub fn as_bytes(self) -> [u8; HEADER_SIZE] {
        let mut buf = [0; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic.to_le_bytes());
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.min_lsn.to_le_bytes());
        buf[24..32].copy_from_slice(&self.max_lsn.to_le_bytes());
        buf
    }

    const fn from_bytes(bytes: [u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            version: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            entry_count: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            min_lsn: u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                bytes[23],
            ]),
            max_lsn: u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
                bytes[31],
            ]),
        }
    }

    const fn validate(&self) -> Result<(), Error> {
        if self.magic != MAGIC_HEADER {
            return Err(Error::InvalidMagic);
        }
        if self.version != VERSION {
            return Err(Error::UnsupportedVersion { found: self.version, expected: VERSION });
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Footer {
    data_crc: u32,
    reserved: u32,
    magic: u64,
}

impl Footer {
    pub const fn new(data_crc: u32) -> Self {
        Self { data_crc, reserved: 0, magic: MAGIC_FOOTER }
    }

    pub fn as_bytes(self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&self.data_crc.to_le_bytes());
        buf[4..8].copy_from_slice(&self.reserved.to_le_bytes());
        buf[8..16].copy_from_slice(&self.magic.to_le_bytes());
        buf
    }

    const fn from_bytes(bytes: [u8; FOOTER_SIZE]) -> Self {
        Self {
            data_crc: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            reserved: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            magic: u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ]),
        }
    }

    const fn validate(&self, expected_crc: u32) -> Result<(), Error> {
        if self.magic != MAGIC_FOOTER {
            return Err(Error::InvalidMagic);
        }
        if self.data_crc != expected_crc {
            return Err(Error::CorruptedData);
        }
        Ok(())
    }
}

pub struct SstableInfo {
    pub path: PathBuf,
    pub entry_count: u32,
    pub min_lsn: u64,
    pub max_lsn: u64,
}

pub fn flush_memtable(memtable: &Memtable, final_path: &Path) -> Result<SstableInfo, Error> {
    if memtable.is_empty() {
        return Err(Error::EmptyMemtable);
    }

    let temp_path = temp_path(final_path);

    let file = OpenOptions::new().write(true).create_new(true).open(&temp_path)?;

    let mut writer = BufWriter::new(file);
    let mut hasher = crc32fast::Hasher::new();

    let mut entry_count = 0;
    let mut min_lsn = u64::MAX;
    let mut max_lsn = 0;
    let mut prev_lsn = 0;

    // Entry count and LSN range are unknown until iteration completes. Writing zeros here
    // ensures the file has an invalid magic number if a crash occurs before the final header.
    let placeholder_header = [0; HEADER_SIZE];
    writer.write_all(&placeholder_header)?;

    for entry in memtable.iter() {
        if entry_count > 0 && entry.lsn <= prev_lsn {
            drop(writer);
            let _ = remove_file(&temp_path);
            return Err(Error::LsnNotMonotonic { current: entry.lsn, previous: prev_lsn });
        }

        let lsn_bytes = entry.lsn.to_le_bytes();
        let timestamp_bytes = entry.timestamp_ms.to_le_bytes();
        let length_bytes = (entry.data.len() as u32).to_le_bytes();

        hasher.update(&lsn_bytes);
        hasher.update(&timestamp_bytes);
        hasher.update(&length_bytes);
        hasher.update(&entry.data);

        writer.write_all(&lsn_bytes)?;
        writer.write_all(&timestamp_bytes)?;
        writer.write_all(&length_bytes)?;
        writer.write_all(&entry.data)?;

        min_lsn = min_lsn.min(entry.lsn);
        max_lsn = max_lsn.max(entry.lsn);
        prev_lsn = entry.lsn;
        entry_count += 1;
    }

    let data_crc = hasher.finalize();
    let footer = Footer::new(data_crc);
    writer.write_all(&footer.as_bytes())?;

    writer.flush()?;

    // BufWriter must be consumed to access the underlying File for fsync. The file is then
    // closed and reopened so the header write starts at offset 0. Data must be durable before
    // the header is written; otherwise a crash could leave a valid header pointing to garbage.
    let file = writer.into_inner().map_err(|e| Error::Io(e.into_error()))?;
    sync_file(&file);
    drop(file);

    let header = Header { magic: MAGIC_HEADER, version: VERSION, entry_count, min_lsn, max_lsn };
    let mut file = OpenOptions::new().write(true).open(&temp_path)?;
    file.write_all(&header.as_bytes())?;
    sync_file(&file);
    drop(file);

    atomic_rename(&temp_path, final_path)?;

    Ok(SstableInfo { path: final_path.to_path_buf(), entry_count, min_lsn, max_lsn })
}

pub fn cleanup_temp_files(dir: &Path) -> Result<usize, Error> {
    let mut cleaned = 0;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.to_str().is_some_and(|s| s.ends_with(".sst.tmp")) {
            remove_file(&path).map_err(Error::TempFileCleanup)?;
            cleaned += 1;
        }
    }

    Ok(cleaned)
}

pub struct SstableReader {
    reader: BufReader<File>,
    header: Header,
    hasher: crc32fast::Hasher,
    entries_read: u32,
}

impl SstableReader {
    pub fn open(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header_bytes = [0; HEADER_SIZE];
        reader.read_exact(&mut header_bytes)?;
        let header = Header::from_bytes(header_bytes);
        header.validate()?;

        Ok(Self { reader, header, hasher: crc32fast::Hasher::new(), entries_read: 0 })
    }

    #[must_use]
    pub const fn entry_count(&self) -> u32 {
        self.header.entry_count
    }

    #[must_use]
    pub const fn min_lsn(&self) -> u64 {
        self.header.min_lsn
    }

    #[must_use]
    pub const fn max_lsn(&self) -> u64 {
        self.header.max_lsn
    }

    pub fn read_entry(&mut self) -> Result<Option<Entry>, Error> {
        if self.entries_read >= self.header.entry_count {
            return Ok(None);
        }

        let mut lsn_bytes = [0; 8];
        let mut timestamp_bytes = [0; 8];
        let mut length_bytes = [0; 4];

        self.reader.read_exact(&mut lsn_bytes)?;
        self.reader.read_exact(&mut timestamp_bytes)?;
        self.reader.read_exact(&mut length_bytes)?;

        let lsn = u64::from_le_bytes(lsn_bytes);
        let timestamp_ms = u64::from_le_bytes(timestamp_bytes);
        let len = u32::from_le_bytes(length_bytes) as usize;

        if len > MAX_ENTRY_SIZE {
            return Err(Error::EntryTooLarge { size: len, max: MAX_ENTRY_SIZE });
        }

        let mut data = vec![0; len];
        self.reader.read_exact(&mut data)?;

        self.hasher.update(&lsn_bytes);
        self.hasher.update(&timestamp_bytes);
        self.hasher.update(&length_bytes);
        self.hasher.update(&data);

        self.entries_read += 1;

        Ok(Some(Entry { lsn, timestamp_ms, data }))
    }

    pub fn validate_footer(mut self) -> Result<(), Error> {
        while self.read_entry()?.is_some() {}

        let computed_crc = self.hasher.finalize();

        let mut footer_bytes = [0; FOOTER_SIZE];
        self.reader.read_exact(&mut footer_bytes)?;
        let footer = Footer::from_bytes(footer_bytes);

        footer.validate(computed_crc)
    }

    pub fn into_entries(mut self) -> Result<Vec<Entry>, Error> {
        let mut entries = Vec::with_capacity(self.header.entry_count as usize);

        while let Some(entry) = self.read_entry()? {
            entries.push(entry);
        }

        let computed_crc = self.hasher.finalize();

        let mut footer_bytes = [0; FOOTER_SIZE];
        self.reader.read_exact(&mut footer_bytes)?;
        let footer = Footer::from_bytes(footer_bytes);
        footer.validate(computed_crc)?;

        Ok(entries)
    }

    #[must_use]
    pub const fn iter(self) -> SstableIter {
        SstableIter { reader: self, error: None, finished: false }
    }
}

impl IntoIterator for SstableReader {
    type Item = Entry;
    type IntoIter = SstableIter;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct SstableIter {
    reader: SstableReader,
    error: Option<Error>,
    finished: bool,
}

impl SstableIter {
    #[must_use]
    pub fn take_error(&mut self) -> Option<Error> {
        self.error.take()
    }

    pub fn validate_crc(mut self) -> Result<(), Error> {
        while self.next().is_some() {}

        if let Some(e) = self.error {
            return Err(e);
        }

        let computed_crc = self.reader.hasher.finalize();

        let mut footer_bytes = [0; FOOTER_SIZE];
        if let Err(e) = self.reader.reader.read_exact(&mut footer_bytes) {
            return Err(Error::Io(e));
        }

        Footer::from_bytes(footer_bytes).validate(computed_crc)
    }
}

impl Iterator for SstableIter {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.error.is_some() {
            return None;
        }

        match self.reader.read_entry() {
            Ok(Some(entry)) => Some(entry),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(e) => {
                self.error = Some(e);
                self.finished = true;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.reader.header.entry_count - self.reader.entries_read) as usize;
        (remaining, Some(remaining))
    }
}

#[must_use]
pub fn sstable_name(min_lsn: u64, max_lsn: u64) -> String {
    format!("{min_lsn:016x}_{max_lsn:016x}.sst")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = Header {
            magic: MAGIC_HEADER,
            version: VERSION,
            entry_count: 42,
            min_lsn: 1,
            max_lsn: 100,
        };

        let bytes = header.as_bytes();
        let restored = Header::from_bytes(bytes);

        assert_eq!(restored.magic, MAGIC_HEADER);
        assert_eq!(restored.version, VERSION);
        assert_eq!(restored.entry_count, 42);
        assert_eq!(restored.min_lsn, 1);
        assert_eq!(restored.max_lsn, 100);
    }

    #[test]
    fn test_footer_roundtrip() {
        let footer = Footer::new(0xDEAD_BEEF);
        let bytes = footer.as_bytes();
        let restored = Footer::from_bytes(bytes);

        assert_eq!(restored.data_crc, 0xDEAD_BEEF);
        assert_eq!(restored.magic, MAGIC_FOOTER);
    }

    #[test]
    fn test_header_validation_invalid_magic() {
        let header =
            Header { magic: 0xBAD, version: VERSION, entry_count: 0, min_lsn: 0, max_lsn: 0 };

        assert!(matches!(header.validate(), Err(Error::InvalidMagic)));
    }

    #[test]
    fn test_header_validation_unsupported_version() {
        let header =
            Header { magic: MAGIC_HEADER, version: 99, entry_count: 0, min_lsn: 0, max_lsn: 0 };

        assert!(matches!(
            header.validate(),
            Err(Error::UnsupportedVersion { found: 99, expected: 1 })
        ));
    }

    #[test]
    fn test_footer_validation_invalid_crc() {
        let footer = Footer::new(0xDEAD_BEEF);
        assert!(matches!(footer.validate(0xCAFE_BABE), Err(Error::CorruptedData)));
    }

    #[test]
    fn test_flush_empty_memtable_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");
        let memtable = Memtable::new();

        assert!(matches!(flush_memtable(&memtable, &path), Err(Error::EmptyMemtable)));
    }

    #[test]
    fn test_flush_and_read_single_entry() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");

        let mut memtable = Memtable::new();
        memtable.insert(1, 1000, vec![0xAB; 100]);

        let info = flush_memtable(&memtable, &path).unwrap();

        assert_eq!(info.entry_count, 1);
        assert_eq!(info.min_lsn, 1);
        assert_eq!(info.max_lsn, 1);
        assert!(path.exists());

        let reader = SstableReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 1);
        assert_eq!(reader.min_lsn(), 1);
        assert_eq!(reader.max_lsn(), 1);

        let entries = reader.into_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].lsn, 1);
        assert_eq!(entries[0].timestamp_ms, 1000);
        assert_eq!(entries[0].data, vec![0xAB; 100]);
    }

    #[test]
    fn test_flush_and_read_multiple_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");

        let mut memtable = Memtable::new();
        memtable.insert(10, 1000, vec![0x0A; 50]);
        memtable.insert(20, 2000, vec![0x14; 100]);
        memtable.insert(30, 3000, vec![0x1E; 150]);

        let info = flush_memtable(&memtable, &path).unwrap();

        assert_eq!(info.entry_count, 3);
        assert_eq!(info.min_lsn, 10);
        assert_eq!(info.max_lsn, 30);

        let entries = SstableReader::open(&path).unwrap().into_entries().unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].lsn, 10);
        assert_eq!(entries[1].lsn, 20);
        assert_eq!(entries[2].lsn, 30);
        assert_eq!(entries[0].data.len(), 50);
        assert_eq!(entries[1].data.len(), 100);
        assert_eq!(entries[2].data.len(), 150);
    }

    #[test]
    fn test_temp_file_not_left_on_success() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");

        let mut memtable = Memtable::new();
        memtable.insert(1, 1000, vec![0x01]);

        flush_memtable(&memtable, &path).unwrap();

        let temp = temp_path(&path);
        assert!(!temp.exists());
        assert!(path.exists());
    }

    #[test]
    fn test_cleanup_temp_files() {
        let temp_dir = tempfile::tempdir().unwrap();

        fs::write(temp_dir.path().join("orphan1.sst.tmp"), b"garbage").unwrap();
        fs::write(temp_dir.path().join("orphan2.sst.tmp"), b"garbage").unwrap();
        fs::write(temp_dir.path().join("valid.sst"), b"data").unwrap();
        fs::write(temp_dir.path().join("other.tmp"), b"data").unwrap();

        let cleaned = cleanup_temp_files(temp_dir.path()).unwrap();

        assert_eq!(cleaned, 2);
        assert!(!temp_dir.path().join("orphan1.sst.tmp").exists());
        assert!(!temp_dir.path().join("orphan2.sst.tmp").exists());
        assert!(temp_dir.path().join("valid.sst").exists());
        assert!(temp_dir.path().join("other.tmp").exists());
    }

    #[test]
    fn test_validate_footer_detects_corruption() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");

        let mut memtable = Memtable::new();
        memtable.insert(1, 1000, vec![0x01; 100]);

        flush_memtable(&memtable, &path).unwrap();

        let mut contents = fs::read(&path).unwrap();
        let data_offset = HEADER_SIZE + 8 + 8 + 4;
        contents[data_offset + 50] ^= 0xFF;
        fs::write(&path, contents).unwrap();

        let reader = SstableReader::open(&path).unwrap();
        let result = reader.validate_footer();
        assert!(
            matches!(result, Err(Error::CorruptedData)),
            "expected CorruptedData, got {result:?}"
        );
    }

    #[test]
    fn test_sstable_name_format() {
        let name = sstable_name(1, 100);
        assert_eq!(name, "0000000000000001_0000000000000064.sst");
    }

    #[test]
    fn test_incremental_crc_validation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");

        let mut memtable = Memtable::new();
        for i in 1..=10 {
            memtable.insert(i, 1000 + i, vec![i as u8; (i * 10) as usize]);
        }

        flush_memtable(&memtable, &path).unwrap();

        let reader = SstableReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 10);
        assert!(reader.validate_footer().is_ok());
    }

    #[test]
    fn test_entry_too_large_rejected() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");

        let mut memtable = Memtable::new();
        memtable.insert(1, 1000, vec![0xAB; 100]);

        flush_memtable(&memtable, &path).unwrap();

        let mut contents = fs::read(&path).unwrap();
        let len_offset = HEADER_SIZE + 8 + 8;
        let huge_len = (MAX_ENTRY_SIZE + 1) as u32;
        contents[len_offset..len_offset + 4].copy_from_slice(&huge_len.to_le_bytes());
        fs::write(&path, contents).unwrap();
        let mut reader = SstableReader::open(&path).unwrap();
        let result = reader.read_entry();
        assert!(
            matches!(result, Err(Error::EntryTooLarge { size, max }) if size == MAX_ENTRY_SIZE + 1 && max == MAX_ENTRY_SIZE),
            "expected EntryTooLarge, got {result:?}"
        );
    }
}
