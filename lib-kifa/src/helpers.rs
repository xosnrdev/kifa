use std::cmp::Ordering;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::KIBI;

pub const SECTOR_SIZE: usize = 4 * KIBI; // 4KiB

pub const VERSION: u32 = 1;

#[cfg(not(target_os = "linux"))]
pub fn sync_file(file: &File) {
    file.sync_all().expect("FATAL: fsync failed");
}

#[cfg(target_os = "linux")]
pub fn sync_file(file: &File) {
    file.sync_data().expect("FATAL: fdatasync failed");
}

#[cfg(unix)]
pub fn sync_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(unix)]
pub fn sync_dir_path(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[allow(clippy::unnecessary_wraps)]
#[cfg(windows)]
pub fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
#[cfg(windows)]
pub fn sync_dir_path(_path: &Path) -> io::Result<()> {
    Ok(())
}

pub struct HeapEntry {
    pub timestamp_ns: u64,
    pub data: Arc<[u8]>,
    pub source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_ns == other.timestamp_ns
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp_ns.cmp(&other.timestamp_ns)
    }
}
