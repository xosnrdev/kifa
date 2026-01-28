//! Snapshot-based manifest for tracking `SSTable` files and WAL checkpoint state.
//!
//! Unlike `LevelDB` or `RocksDB`, which use append-only logs of version edits, this manifest
//! rewrites the full state on each save. The simpler approach works here because updates are
//! infrequent (only on memtable flush or compaction) and the file remains small. The checkpoint
//! LSN indicates the highest LSN durably flushed to `SSTables`, allowing safe WAL truncation.

use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::{fmt, fs};

use crate::common::{atomic_rename, temp_path};
use crate::helpers::{VERSION, sync_file};
use crate::{KIBI, map_err};

const MAGIC_HEADER: u64 = 0x2C6F_E96E_E78B_6955;
const MAGIC_FOOTER: u64 = 0x369D_EA0F_31A5_3F85;
const HEADER_SIZE: usize = 24;
const FOOTER_SIZE: usize = 12;
const MAX_PATH_LEN: usize = 4 * KIBI;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidMagic,
    UnsupportedVersion { found: u32, expected: u32 },
    CorruptedData,
    CheckpointRegression { current: u64, attempted: u64 },
    DuplicateSstable { path: PathBuf },
    MissingSstable { path: PathBuf },
    PathTooLong { len: usize, max: usize },
    InvalidUtf8Path,
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
            Self::CheckpointRegression { current, attempted } => {
                write!(f, "checkpoint regression: {attempted} < {current}")
            }
            Self::DuplicateSstable { path } => {
                write!(f, "duplicate sstable: {}", path.display())
            }
            Self::MissingSstable { path } => {
                write!(f, "missing sstable: {}", path.display())
            }
            Self::PathTooLong { len, max } => {
                write!(f, "path too long: {len} bytes (max {max})")
            }
            Self::InvalidUtf8Path => write!(f, "invalid UTF-8 in path"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstableEntry {
    pub path: PathBuf,
    pub min_lsn: u64,
    pub max_lsn: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Header {
    magic: u64,
    version: u32,
    sstable_count: u32,
    checkpoint_lsn: u64,
}

impl Header {
    fn as_bytes(self) -> [u8; HEADER_SIZE] {
        let mut buf = [0; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic.to_le_bytes());
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.sstable_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf
    }

    const fn from_bytes(bytes: [u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            version: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            sstable_count: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            checkpoint_lsn: u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                bytes[23],
            ]),
        }
    }

    const fn validate(&self) -> Result<(), Error> {
        if self.magic != MAGIC_HEADER {
            return Err(Error::InvalidMagic);
        }
        if self.version > VERSION {
            return Err(Error::UnsupportedVersion { found: self.version, expected: VERSION });
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Footer {
    content_crc: u32,
    magic: u64,
}

impl Footer {
    const fn new(content_crc: u32) -> Self {
        Self { content_crc, magic: MAGIC_FOOTER }
    }

    fn as_bytes(self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&self.content_crc.to_le_bytes());
        buf[4..12].copy_from_slice(&self.magic.to_le_bytes());
        buf
    }

    const fn from_bytes(bytes: [u8; FOOTER_SIZE]) -> Self {
        Self {
            content_crc: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            magic: u64::from_le_bytes([
                bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
            ]),
        }
    }

    const fn validate(&self, expected_crc: u32) -> Result<(), Error> {
        if self.magic != MAGIC_FOOTER {
            return Err(Error::InvalidMagic);
        }
        if self.content_crc != expected_crc {
            return Err(Error::CorruptedData);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Manifest {
    path: PathBuf,
    checkpoint_lsn: u64,
    sstables: Vec<SstableEntry>,
}

impl Manifest {
    #[must_use]
    pub const fn new(path: PathBuf) -> Self {
        Self { path, checkpoint_lsn: 0, sstables: Vec::new() }
    }

    pub fn load(path: &Path, validate_sstables: bool) -> Result<Self, Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut hasher = crc32fast::Hasher::new();

        let mut header_bytes = [0; HEADER_SIZE];
        reader.read_exact(&mut header_bytes)?;
        hasher.update(&header_bytes);

        let header = Header::from_bytes(header_bytes);
        header.validate()?;

        let mut sstables = Vec::with_capacity(header.sstable_count as usize);

        for _ in 0..header.sstable_count {
            let mut min_lsn_bytes = [0; 8];
            let mut max_lsn_bytes = [0; 8];
            let mut path_len_bytes = [0; 2];

            reader.read_exact(&mut min_lsn_bytes)?;
            reader.read_exact(&mut max_lsn_bytes)?;
            reader.read_exact(&mut path_len_bytes)?;

            hasher.update(&min_lsn_bytes);
            hasher.update(&max_lsn_bytes);
            hasher.update(&path_len_bytes);

            let min_lsn = u64::from_le_bytes(min_lsn_bytes);
            let max_lsn = u64::from_le_bytes(max_lsn_bytes);
            let path_len = u16::from_le_bytes(path_len_bytes) as usize;

            if path_len > MAX_PATH_LEN {
                return Err(Error::PathTooLong { len: path_len, max: MAX_PATH_LEN });
            }

            let mut path_bytes = vec![0; path_len];
            reader.read_exact(&mut path_bytes)?;
            hasher.update(&path_bytes);

            let path_str = std::str::from_utf8(&path_bytes).map_err(|_| Error::InvalidUtf8Path)?;
            let sstable_path = PathBuf::from(path_str);

            if validate_sstables && !sstable_path.exists() {
                return Err(Error::MissingSstable { path: sstable_path });
            }

            sstables.push(SstableEntry { path: sstable_path, min_lsn, max_lsn });
        }

        let computed_crc = hasher.finalize();

        let mut footer_bytes = [0; FOOTER_SIZE];
        reader.read_exact(&mut footer_bytes)?;
        let footer = Footer::from_bytes(footer_bytes);
        footer.validate(computed_crc)?;

        Ok(Self { path: path.to_path_buf(), checkpoint_lsn: header.checkpoint_lsn, sstables })
    }

    pub fn save(&self) -> Result<(), Error> {
        let temp_path = temp_path(&self.path);

        let file = OpenOptions::new().write(true).create_new(true).open(&temp_path)?;
        let mut writer = BufWriter::new(file);
        let mut hasher = crc32fast::Hasher::new();

        let header = Header {
            magic: MAGIC_HEADER,
            version: VERSION,
            sstable_count: self.sstables.len() as u32,
            checkpoint_lsn: self.checkpoint_lsn,
        };

        let header_bytes = header.as_bytes();
        hasher.update(&header_bytes);
        writer.write_all(&header_bytes)?;

        for entry in &self.sstables {
            let path_str = entry.path.to_str().ok_or(Error::InvalidUtf8Path)?;
            let path_bytes = path_str.as_bytes();

            if path_bytes.len() > MAX_PATH_LEN {
                return Err(Error::PathTooLong { len: path_bytes.len(), max: MAX_PATH_LEN });
            }

            let min_lsn_bytes = entry.min_lsn.to_le_bytes();
            let max_lsn_bytes = entry.max_lsn.to_le_bytes();
            let path_len_bytes = (path_bytes.len() as u16).to_le_bytes();

            hasher.update(&min_lsn_bytes);
            hasher.update(&max_lsn_bytes);
            hasher.update(&path_len_bytes);
            hasher.update(path_bytes);

            writer.write_all(&min_lsn_bytes)?;
            writer.write_all(&max_lsn_bytes)?;
            writer.write_all(&path_len_bytes)?;
            writer.write_all(path_bytes)?;
        }

        let content_crc = hasher.finalize();
        let footer = Footer::new(content_crc);
        writer.write_all(&footer.as_bytes())?;

        writer.flush()?;
        let file = writer.into_inner().map_err(|e| Error::Io(e.into_error()))?;
        sync_file(&file);
        drop(file);

        atomic_rename(&temp_path, &self.path).map_err(Into::into)
    }

    pub fn register_sstable(
        &mut self,
        path: PathBuf,
        min_lsn: u64,
        max_lsn: u64,
    ) -> Result<(), Error> {
        if self.sstables.iter().any(|e| e.path == path) {
            return Err(Error::DuplicateSstable { path });
        }

        // Registering an SSTable with max_lsn below the checkpoint would allow WAL truncation
        // past LSNs that might not exist in any SSTable, causing data loss on recovery.
        if max_lsn < self.checkpoint_lsn {
            return Err(Error::CheckpointRegression {
                current: self.checkpoint_lsn,
                attempted: max_lsn,
            });
        }

        self.sstables.push(SstableEntry { path, min_lsn, max_lsn });
        self.checkpoint_lsn = self.checkpoint_lsn.max(max_lsn);

        Ok(())
    }

    pub fn remove_sstables(&mut self, paths: &[PathBuf]) {
        self.sstables.retain(|e| !paths.contains(&e.path));
    }

    #[must_use]
    pub const fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn
    }

    #[must_use]
    pub fn sstables(&self) -> &[SstableEntry] {
        &self.sstables
    }

    #[must_use]
    pub const fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn cleanup_temp_manifests(dir: &Path) -> Result<usize, Error> {
    let mut cleaned = 0;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.to_str().is_some_and(|s| s.ends_with(".manifest.tmp")) {
            remove_file(&path)?;
            cleaned += 1;
        }
    }

    Ok(cleaned)
}

#[must_use]
pub fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = Header {
            magic: MAGIC_HEADER,
            version: VERSION,
            sstable_count: 5,
            checkpoint_lsn: 12345,
        };

        let bytes = header.as_bytes();
        let restored = Header::from_bytes(bytes);

        assert_eq!(restored.magic, MAGIC_HEADER);
        assert_eq!(restored.version, VERSION);
        assert_eq!(restored.sstable_count, 5);
        assert_eq!(restored.checkpoint_lsn, 12345);
    }

    #[test]
    fn test_footer_roundtrip() {
        let footer = Footer::new(0xDEAD_BEEF);
        let bytes = footer.as_bytes();
        let restored = Footer::from_bytes(bytes);

        assert_eq!(restored.content_crc, 0xDEAD_BEEF);
        assert_eq!(restored.magic, MAGIC_FOOTER);
    }

    #[test]
    fn test_header_validation_invalid_magic() {
        let header = Header { magic: 0xBAD, version: VERSION, sstable_count: 0, checkpoint_lsn: 0 };

        assert!(matches!(header.validate(), Err(Error::InvalidMagic)));
    }

    #[test]
    fn test_header_validation_unsupported_version() {
        let header =
            Header { magic: MAGIC_HEADER, version: 99, sstable_count: 0, checkpoint_lsn: 0 };

        assert!(matches!(
            header.validate(),
            Err(Error::UnsupportedVersion { found: 99, expected: 1 })
        ));
    }

    #[test]
    fn test_new_manifest_empty() {
        let manifest = Manifest::new(PathBuf::from("/tmp/MANIFEST"));

        assert_eq!(manifest.checkpoint_lsn(), 0);
        assert_eq!(manifest.sstable_count(), 0);
        assert!(manifest.sstables().is_empty());
    }

    #[test]
    fn test_register_sstable() {
        let mut manifest = Manifest::new(PathBuf::from("/tmp/MANIFEST"));

        manifest.register_sstable(PathBuf::from("/data/001.sst"), 1, 100).unwrap();

        assert_eq!(manifest.sstable_count(), 1);
        assert_eq!(manifest.checkpoint_lsn(), 100);
        assert_eq!(manifest.sstables()[0].path, PathBuf::from("/data/001.sst"));
        assert_eq!(manifest.sstables()[0].min_lsn, 1);
        assert_eq!(manifest.sstables()[0].max_lsn, 100);
    }

    #[test]
    fn test_register_multiple_sstables_updates_checkpoint() {
        let mut manifest = Manifest::new(PathBuf::from("/tmp/MANIFEST"));

        manifest.register_sstable(PathBuf::from("/data/001.sst"), 1, 100).unwrap();
        manifest.register_sstable(PathBuf::from("/data/002.sst"), 101, 200).unwrap();
        manifest.register_sstable(PathBuf::from("/data/003.sst"), 201, 300).unwrap();

        assert_eq!(manifest.sstable_count(), 3);
        assert_eq!(manifest.checkpoint_lsn(), 300);
    }

    #[test]
    fn test_register_duplicate_sstable_fails() {
        let mut manifest = Manifest::new(PathBuf::from("/tmp/MANIFEST"));

        manifest.register_sstable(PathBuf::from("/data/001.sst"), 1, 100).unwrap();

        let result = manifest.register_sstable(PathBuf::from("/data/001.sst"), 101, 200);

        assert!(
            matches!(result, Err(Error::DuplicateSstable { ref path }) if path.as_path() == Path::new("/data/001.sst"))
        );
    }

    #[test]
    fn test_checkpoint_regression_fails() {
        let mut manifest = Manifest::new(PathBuf::from("/tmp/MANIFEST"));

        manifest.register_sstable(PathBuf::from("/data/001.sst"), 1, 100).unwrap();

        let result = manifest.register_sstable(PathBuf::from("/data/002.sst"), 50, 80);

        assert!(matches!(result, Err(Error::CheckpointRegression { current: 100, attempted: 80 })));
    }

    #[test]
    fn test_save_and_load_empty_manifest() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("MANIFEST");

        let manifest = Manifest::new(path.clone());
        manifest.save().unwrap();

        let loaded = Manifest::load(&path, false).unwrap();

        assert_eq!(loaded.checkpoint_lsn(), 0);
        assert_eq!(loaded.sstable_count(), 0);
    }

    #[test]
    fn test_save_and_load_with_sstables() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("MANIFEST");

        let sst1 = temp_dir.path().join("001.sst");
        let sst2 = temp_dir.path().join("002.sst");
        fs::write(&sst1, b"dummy").unwrap();
        fs::write(&sst2, b"dummy").unwrap();

        let mut manifest = Manifest::new(path.clone());
        manifest.register_sstable(sst1.clone(), 1, 100).unwrap();
        manifest.register_sstable(sst2.clone(), 101, 200).unwrap();
        manifest.save().unwrap();

        let loaded = Manifest::load(&path, true).unwrap();

        assert_eq!(loaded.checkpoint_lsn(), 200);
        assert_eq!(loaded.sstable_count(), 2);
        assert_eq!(loaded.sstables()[0].path, sst1);
        assert_eq!(loaded.sstables()[1].path, sst2);
    }

    #[test]
    fn test_load_sstable_validation_flag() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("MANIFEST");

        let mut manifest = Manifest::new(path.clone());
        manifest.register_sstable(PathBuf::from("/nonexistent/001.sst"), 1, 100).unwrap();
        manifest.save().unwrap();

        let result = Manifest::load(&path, true);
        assert!(
            matches!(result, Err(Error::MissingSstable { ref path }) if path.as_path() == Path::new("/nonexistent/001.sst"))
        );

        let loaded = Manifest::load(&path, false).unwrap();
        assert_eq!(loaded.sstable_count(), 1);
    }

    #[test]
    fn test_corrupted_manifest_detected() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("MANIFEST");

        let sst = temp_dir.path().join("001.sst");
        fs::write(&sst, b"dummy").unwrap();

        let mut manifest = Manifest::new(path.clone());
        manifest.register_sstable(sst, 1, 100).unwrap();
        manifest.save().unwrap();

        let mut contents = fs::read(&path).unwrap();
        let sstable_entry_start = HEADER_SIZE;
        contents[sstable_entry_start + 5] ^= 0xFF;
        fs::write(&path, contents).unwrap();

        let result = Manifest::load(&path, false);

        assert!(
            matches!(result, Err(Error::CorruptedData)),
            "expected CorruptedData, got {result:?}"
        );
    }

    #[test]
    fn test_remove_sstables() {
        let mut manifest = Manifest::new(PathBuf::from("/tmp/MANIFEST"));

        manifest.register_sstable(PathBuf::from("/data/001.sst"), 1, 100).unwrap();
        manifest.register_sstable(PathBuf::from("/data/002.sst"), 101, 200).unwrap();
        manifest.register_sstable(PathBuf::from("/data/003.sst"), 201, 300).unwrap();

        manifest.remove_sstables(&[PathBuf::from("/data/001.sst"), PathBuf::from("/data/002.sst")]);

        assert_eq!(manifest.sstable_count(), 1);
        assert_eq!(manifest.sstables()[0].path, PathBuf::from("/data/003.sst"));
        assert_eq!(manifest.checkpoint_lsn(), 300);
    }

    #[test]
    fn test_temp_file_not_left_on_success() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("MANIFEST");

        let manifest = Manifest::new(path.clone());
        manifest.save().unwrap();

        let temp = temp_path(&path);
        assert!(!temp.exists());
        assert!(path.exists());
    }

    #[test]
    fn test_cleanup_temp_manifests() {
        let temp_dir = tempdir().unwrap();

        fs::write(temp_dir.path().join("MANIFEST.tmp"), b"garbage").unwrap();
        fs::write(temp_dir.path().join("other.manifest.tmp"), b"garbage").unwrap();
        fs::write(temp_dir.path().join("MANIFEST"), b"valid").unwrap();

        let cleaned = cleanup_temp_manifests(temp_dir.path()).unwrap();

        assert_eq!(cleaned, 1);
        assert!(!temp_dir.path().join("other.manifest.tmp").exists());
        assert!(temp_dir.path().join("MANIFEST.tmp").exists());
        assert!(temp_dir.path().join("MANIFEST").exists());
    }

    #[test]
    fn test_manifest_path_helper() {
        let dir = Path::new("/data/kifa");
        let path = manifest_path(dir);

        assert_eq!(path, PathBuf::from("/data/kifa/MANIFEST"));
    }

    #[test]
    fn test_idempotent_load() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("MANIFEST");

        let sst = temp_dir.path().join("001.sst");
        fs::write(&sst, b"dummy").unwrap();

        let mut manifest = Manifest::new(path.clone());
        manifest.register_sstable(sst.clone(), 1, 100).unwrap();
        manifest.save().unwrap();

        let loaded1 = Manifest::load(&path, true).unwrap();
        let loaded2 = Manifest::load(&path, true).unwrap();

        assert_eq!(loaded1.checkpoint_lsn(), loaded2.checkpoint_lsn());
        assert_eq!(loaded1.sstable_count(), loaded2.sstable_count());
        assert_eq!(loaded1.sstables(), loaded2.sstables());
    }
}
