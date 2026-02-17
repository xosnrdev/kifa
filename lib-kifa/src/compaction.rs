//! Full-merge compaction for consolidating `SSTables` into a single sorted file.
//!
//! Unlike leveled compaction in `LevelDB` or `RocksDB`, this merges all input `SSTables` at once
//! using a k-way merge via min-heap. Entries remain ordered by monotonic nanosecond timestamp,
//! matching the append-only log workload. The two-phase design separates file creation from
//! manifest updates, allowing safe rollback if either step fails.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::common::{atomic_rename, temp_path};
use crate::helpers::{HeapEntry, VERSION, sync_file};
use crate::manifest::{self, Manifest, SstableEntry};
use crate::sstable::{
    Footer, HEADER_SIZE, Header, MAGIC_HEADER, MAX_ENTRY_SIZE, SstableIter, SstableReader,
    sstable_name,
};
use crate::{MEBI, map_err, sstable};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Sstable(sstable::Error),
    Manifest(manifest::Error),
    NoInputSstables,
    EntryTooLarge { size: usize, max: usize },
}

map_err!(Io, io::Error);
map_err!(Sstable, sstable::Error);
map_err!(Manifest, manifest::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Sstable(e) => write!(f, "sstable: {e}"),
            Self::Manifest(e) => write!(f, "manifest: {e}"),
            Self::NoInputSstables => write!(f, "no input sstables"),
            Self::EntryTooLarge { size, max } => {
                write!(f, "entry too large: {} MiB (max {} MiB)", size / MEBI, max / MEBI)
            }
        }
    }
}

impl std::error::Error for Error {}

pub struct CompactionResult {
    pub input_count: usize,
    pub output_path: PathBuf,
    pub entry_count: u32,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    pub removed_paths: Vec<PathBuf>,
}

pub struct CompactionOutput {
    pub output_path: PathBuf,
    pub entry_count: u32,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    pub input_paths: Vec<PathBuf>,
}

struct MergeStats {
    entry_count: u32,
    min_timestamp_ns: u64,
    max_timestamp_ns: u64,
    data_crc: u32,
}

fn merge_entries(
    heap: &mut BinaryHeap<Reverse<HeapEntry>>,
    iters: &mut [SstableIter],
    writer: &mut BufWriter<File>,
) -> Result<MergeStats, Error> {
    let mut hasher = crc32fast::Hasher::new();
    let mut entry_count = 0;
    let mut min_timestamp_ns = u64::MAX;
    let mut max_timestamp_ns = 0;
    let mut prev_timestamp_ns = 0;

    while let Some(Reverse(heap_entry)) = heap.pop() {
        if entry_count > 0 && heap_entry.timestamp_ns <= prev_timestamp_ns {
            if let Some(next_entry) = iters[heap_entry.source_idx].next() {
                heap.push(Reverse(HeapEntry {
                    timestamp_ns: next_entry.timestamp_ns,
                    data: next_entry.data,
                    source_idx: heap_entry.source_idx,
                }));
            }
            continue;
        }

        if heap_entry.data.len() > MAX_ENTRY_SIZE {
            return Err(Error::EntryTooLarge { size: heap_entry.data.len(), max: MAX_ENTRY_SIZE });
        }

        let timestamp_bytes = heap_entry.timestamp_ns.to_le_bytes();
        let length_bytes = (heap_entry.data.len() as u32).to_le_bytes();

        hasher.update(&timestamp_bytes);
        hasher.update(&length_bytes);
        hasher.update(&heap_entry.data);

        writer.write_all(&timestamp_bytes)?;
        writer.write_all(&length_bytes)?;
        writer.write_all(&heap_entry.data)?;

        min_timestamp_ns = min_timestamp_ns.min(heap_entry.timestamp_ns);
        max_timestamp_ns = max_timestamp_ns.max(heap_entry.timestamp_ns);
        prev_timestamp_ns = heap_entry.timestamp_ns;
        entry_count += 1;

        if let Some(next_entry) = iters[heap_entry.source_idx].next() {
            heap.push(Reverse(HeapEntry {
                timestamp_ns: next_entry.timestamp_ns,
                data: next_entry.data,
                source_idx: heap_entry.source_idx,
            }));
        }
    }

    for iter in iters {
        if let Some(e) = iter.take_error() {
            return Err(e.into());
        }
    }

    Ok(MergeStats { entry_count, min_timestamp_ns, max_timestamp_ns, data_crc: hasher.finalize() })
}

fn finalize_sstable(
    writer: BufWriter<File>,
    temp_output: &Path,
    output_path: &Path,
    stats: &MergeStats,
) -> Result<(), Error> {
    let footer = Footer::new(stats.data_crc);
    let mut writer = writer;
    writer.write_all(&footer.as_bytes())?;
    writer.flush()?;

    let file = writer.into_inner().map_err(|e| Error::Io(e.into_error()))?;
    sync_file(&file);
    drop(file);

    let header = Header {
        magic: MAGIC_HEADER,
        version: VERSION,
        entry_count: stats.entry_count,
        min_timestamp_ns: stats.min_timestamp_ns,
        max_timestamp_ns: stats.max_timestamp_ns,
    };

    // Reopens the file to overwrite the placeholder header now that stats are known.
    // Data is already synced above, so the header will only reference durable content.
    let mut file = OpenOptions::new().write(true).open(temp_output)?;
    file.write_all(&header.as_bytes())?;
    sync_file(&file);
    drop(file);

    atomic_rename(temp_output, output_path).map_err(Into::into)
}

#[must_use]
pub fn prepare_compaction(
    sstables: &[SstableEntry],
    min_count: usize,
) -> Option<Vec<SstableEntry>> {
    if sstables.len() < min_count {
        return None;
    }
    Some(sstables.to_vec())
}

pub fn run_compaction(dir: &Path, inputs: &[SstableEntry]) -> Result<CompactionOutput, Error> {
    if inputs.is_empty() {
        return Err(Error::NoInputSstables);
    }

    let mut iters = Vec::with_capacity(inputs.len());
    let mut heap = BinaryHeap::new();

    for (idx, entry) in inputs.iter().enumerate() {
        let reader = SstableReader::open(&entry.path)?;
        let mut iter = reader.into_iter();

        if let Some(first) = iter.next() {
            heap.push(Reverse(HeapEntry {
                timestamp_ns: first.timestamp_ns,
                data: first.data,
                source_idx: idx,
            }));
        }
        iters.push(iter);
    }

    let global_min_ts = inputs.iter().map(|e| e.min_timestamp_ns).min().unwrap_or_default();
    let global_max_ts = inputs.iter().map(|e| e.max_timestamp_ns).max().unwrap_or_default();
    let output_name = sstable_name(global_min_ts, global_max_ts);
    let output_path = dir.join(&output_name);
    let temp_output = temp_path(&output_path);

    let file = OpenOptions::new().write(true).create_new(true).open(&temp_output)?;
    let mut writer = BufWriter::new(file);

    // Reserves space for the header. Entry count and timestamp bounds are unknown until merge
    // completes, so finalize_sstable reopens the file to write the real header.
    let placeholder_header = [0; HEADER_SIZE];
    writer.write_all(&placeholder_header)?;

    let stats = match merge_entries(&mut heap, &mut iters, &mut writer) {
        Ok(s) => s,
        Err(e) => {
            drop(writer);
            let _ = remove_file(&temp_output);
            return Err(e);
        }
    };

    if let Err(e) = finalize_sstable(writer, &temp_output, &output_path, &stats) {
        let _ = remove_file(&temp_output);
        return Err(e);
    }

    let input_paths: Vec<_> = inputs.iter().map(|e| e.path.clone()).collect();

    Ok(CompactionOutput {
        output_path,
        entry_count: stats.entry_count,
        min_timestamp_ns: stats.min_timestamp_ns,
        max_timestamp_ns: stats.max_timestamp_ns,
        input_paths,
    })
}

pub fn commit_compaction(
    manifest: &mut Manifest,
    output: CompactionOutput,
) -> Result<CompactionResult, Error> {
    let removed_paths: Vec<_> =
        output.input_paths.iter().filter(|p| **p != output.output_path).cloned().collect();

    manifest.remove_sstables(&removed_paths);

    let already_registered = output.input_paths.contains(&output.output_path);
    if !already_registered {
        manifest.register_sstable(
            output.output_path.clone(),
            output.min_timestamp_ns,
            output.max_timestamp_ns,
        )?;
    }
    manifest.save()?;

    for path in &removed_paths {
        let _ = remove_file(path);
    }

    Ok(CompactionResult {
        input_count: output.input_paths.len(),
        output_path: output.output_path,
        entry_count: output.entry_count,
        min_timestamp_ns: output.min_timestamp_ns,
        max_timestamp_ns: output.max_timestamp_ns,
        removed_paths,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::memtable::Memtable;
    use crate::sstable::flush_memtable;

    fn compact_sstables(
        dir: &Path,
        inputs: &[SstableEntry],
        manifest: &mut Manifest,
    ) -> Result<CompactionResult, Error> {
        let output = run_compaction(dir, inputs)?;
        commit_compaction(manifest, output)
    }

    fn create_sstable(dir: &Path, ts_start: u64, count: u64) -> SstableEntry {
        let mut memtable = Memtable::new();
        for i in 0..count {
            let ts = ts_start + i;
            memtable.insert(ts, Arc::from([ts as u8; 10]));
        }

        let min_ts = ts_start;
        let max_ts = ts_start + count - 1;
        let path = dir.join(sstable_name(min_ts, max_ts));
        flush_memtable(&memtable, &path).unwrap();

        SstableEntry { path, min_timestamp_ns: min_ts, max_timestamp_ns: max_ts }
    }

    #[test]
    fn test_compact_single_sstable_is_noop() {
        let temp_dir = tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(manifest_path);

        let entry = create_sstable(temp_dir.path(), 1000, 10);
        manifest
            .register_sstable(entry.path.clone(), entry.min_timestamp_ns, entry.max_timestamp_ns)
            .unwrap();

        let inputs = manifest.sstables().to_vec();
        let result = compact_sstables(temp_dir.path(), &inputs, &mut manifest).unwrap();

        assert_eq!(result.input_count, 1);
        assert_eq!(result.entry_count, 10);
        assert_eq!(result.output_path, entry.path);
        // Single SSTable compaction produces same output path, so nothing is removed.
        assert!(result.removed_paths.is_empty());
    }

    #[test]
    fn test_compact_multiple_sstables() {
        let temp_dir = tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(manifest_path);

        let entry1 = create_sstable(temp_dir.path(), 1000, 5);
        let entry2 = create_sstable(temp_dir.path(), 1005, 5);
        let entry3 = create_sstable(temp_dir.path(), 1010, 5);

        manifest
            .register_sstable(entry1.path.clone(), entry1.min_timestamp_ns, entry1.max_timestamp_ns)
            .unwrap();
        manifest
            .register_sstable(entry2.path.clone(), entry2.min_timestamp_ns, entry2.max_timestamp_ns)
            .unwrap();
        manifest
            .register_sstable(entry3.path.clone(), entry3.min_timestamp_ns, entry3.max_timestamp_ns)
            .unwrap();

        let inputs = manifest.sstables().to_vec();
        let result = compact_sstables(temp_dir.path(), &inputs, &mut manifest).unwrap();

        assert_eq!(result.input_count, 3);
        assert_eq!(result.entry_count, 15);
        assert_eq!(result.min_timestamp_ns, 1000);
        assert_eq!(result.max_timestamp_ns, 1014);
        assert_eq!(manifest.sstable_count(), 1);

        for path in &result.removed_paths {
            assert!(!path.exists());
        }
    }

    #[test]
    fn test_compact_preserves_timestamp_order() {
        let temp_dir = tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(manifest_path);

        let entry1 = create_sstable(temp_dir.path(), 1000, 5);
        let entry2 = create_sstable(temp_dir.path(), 1010, 5);

        manifest
            .register_sstable(entry1.path.clone(), entry1.min_timestamp_ns, entry1.max_timestamp_ns)
            .unwrap();
        manifest
            .register_sstable(entry2.path.clone(), entry2.min_timestamp_ns, entry2.max_timestamp_ns)
            .unwrap();

        let inputs = manifest.sstables().to_vec();
        let result = compact_sstables(temp_dir.path(), &inputs, &mut manifest).unwrap();

        let reader = SstableReader::open(&result.output_path).unwrap();
        let entries = reader.into_entries().unwrap();

        assert_eq!(entries.len(), 10);
        for i in 1..entries.len() {
            assert!(entries[i].timestamp_ns > entries[i - 1].timestamp_ns);
        }
    }

    #[test]
    fn test_compact_no_inputs_fails() {
        let temp_dir = tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(manifest_path);

        let result = compact_sstables(temp_dir.path(), &[], &mut manifest);
        assert!(matches!(result, Err(Error::NoInputSstables)));
    }

    #[test]
    fn test_compact_manifest_updated_atomically() {
        let temp_dir = tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(manifest_path.clone());

        let entry1 = create_sstable(temp_dir.path(), 1000, 5);
        let entry2 = create_sstable(temp_dir.path(), 1005, 5);

        manifest
            .register_sstable(entry1.path.clone(), entry1.min_timestamp_ns, entry1.max_timestamp_ns)
            .unwrap();
        manifest
            .register_sstable(entry2.path.clone(), entry2.min_timestamp_ns, entry2.max_timestamp_ns)
            .unwrap();
        manifest.save().unwrap();

        let inputs = manifest.sstables().to_vec();
        compact_sstables(temp_dir.path(), &inputs, &mut manifest).unwrap();

        let reloaded = Manifest::load(&manifest_path, true).unwrap();
        assert_eq!(reloaded.sstable_count(), 1);
        assert_eq!(reloaded.checkpoint_timestamp_ns(), 1009);
    }

    #[test]
    fn test_prepare_compaction_threshold() {
        let one_entry = vec![SstableEntry {
            path: PathBuf::from("a.sst"),
            min_timestamp_ns: 1000,
            max_timestamp_ns: 1009,
        }];
        assert!(prepare_compaction(&one_entry, 2).is_none());

        let two_entries = vec![
            SstableEntry {
                path: PathBuf::from("a.sst"),
                min_timestamp_ns: 1000,
                max_timestamp_ns: 1009,
            },
            SstableEntry {
                path: PathBuf::from("b.sst"),
                min_timestamp_ns: 1010,
                max_timestamp_ns: 1019,
            },
        ];
        let candidates = prepare_compaction(&two_entries, 2).unwrap();
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn test_run_and_commit_separate_phases() {
        let temp_dir = tempdir().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(manifest_path);

        let entry1 = create_sstable(temp_dir.path(), 1000, 5);
        let entry2 = create_sstable(temp_dir.path(), 1005, 5);

        manifest
            .register_sstable(entry1.path.clone(), entry1.min_timestamp_ns, entry1.max_timestamp_ns)
            .unwrap();
        manifest
            .register_sstable(entry2.path.clone(), entry2.min_timestamp_ns, entry2.max_timestamp_ns)
            .unwrap();

        let inputs = manifest.sstables().to_vec();

        let output = run_compaction(temp_dir.path(), &inputs).unwrap();
        assert!(output.output_path.exists());
        assert_eq!(output.entry_count, 10);
        assert_eq!(manifest.sstable_count(), 2);

        let result = commit_compaction(&mut manifest, output).unwrap();
        assert_eq!(manifest.sstable_count(), 1);
        assert_eq!(result.removed_paths.len(), 2);
    }
}
