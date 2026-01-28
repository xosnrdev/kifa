use std::cmp::Ordering;
use std::fs::{File, rename};
use std::io;
use std::path::{Path, PathBuf};

pub const KIBI: usize = 1024; // 1KB

pub const MEBI: usize = KIBI * KIBI; // 1MB

pub(crate) const SECTOR_SIZE: usize = 4 * KIBI; // 4KB

pub(crate) const VERSION: u32 = 1;

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

#[cfg(not(target_os = "linux"))]
pub(crate) fn sync_file(file: &File) {
    file.sync_all().expect("FATAL: fsync failed");
}

#[cfg(target_os = "linux")]
pub(crate) fn sync_file(file: &File) {
    file.sync_data().expect("FATAL: fdatasync failed");
}

#[must_use]
pub fn temp_path(final_path: &Path) -> PathBuf {
    let mut temp = final_path.as_os_str().to_owned();
    temp.push(".tmp");
    PathBuf::from(temp)
}

#[cfg(unix)]
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_dir_path(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[allow(clippy::unnecessary_wraps)]
#[cfg(windows)]
pub(crate) fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
#[cfg(windows)]
pub(crate) fn sync_dir_path(_path: &Path) -> io::Result<()> {
    Ok(())
}

pub fn atomic_rename(from: &Path, to: &Path) -> io::Result<()> {
    sync_dir(to)?;
    rename(from, to)?;
    sync_dir(to)
}

pub(crate) struct HeapEntry {
    pub lsn: u64,
    pub timestamp_ms: u64,
    pub data: Vec<u8>,
    pub source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.lsn == other.lsn
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
        self.lsn.cmp(&other.lsn)
    }
}
