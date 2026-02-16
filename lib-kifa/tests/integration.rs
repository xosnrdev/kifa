use std::collections::HashSet;
use std::{fs, thread, time};

use lib_kifa::FlushMode;
use lib_kifa::engine::{Config, StorageEngine};
use tempfile::tempdir;

#[test]
fn test_open_creates_directory() {
    let dir = tempdir().unwrap();
    let engine_dir = dir.path().join("subdir");

    let (engine, report) = StorageEngine::open(&engine_dir, Config::default()).unwrap();

    assert!(engine_dir.exists());
    assert_eq!(report.wal_entries_replayed, 0);
    assert_eq!(report.checkpoint_timestamp_ns, 0);
    assert_eq!(report.sstable_count, 0);
    drop(engine);
}

#[test]
fn test_append_assigns_timestamp_and_populates_memtable() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    let ts1 = engine.append(b"first").unwrap();
    let ts2 = engine.append(b"second").unwrap();

    assert!(ts1 > 0);
    assert!(ts2 > ts1);

    let stats = engine.stats();
    assert_eq!(stats.memtable_entry_count, 2);
    assert!(stats.memtable_size_bytes > 0);
}

#[test]
fn test_flush_creates_sstable() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    engine.append(b"entry1").unwrap();
    engine.append(b"entry2").unwrap();

    let info = engine.flush().unwrap();

    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.entry_count, 2);
    assert!(info.min_timestamp_ns > 0);
    assert!(info.max_timestamp_ns >= info.min_timestamp_ns);
    assert!(info.path.exists());
}

#[test]
fn test_flush_empty_memtable_returns_none() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    let result = engine.flush().unwrap();
    assert!(result.is_none());
}

#[test]
fn test_recovery_replays_wal() {
    let dir = tempdir().unwrap();
    let ts1;
    let ts2;

    {
        let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();
        ts1 = engine.append(b"entry1").unwrap();
        ts2 = engine.append(b"entry2").unwrap();
    }

    let (engine, report) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    assert_eq!(report.wal_entries_replayed, 2);
    assert_eq!(report.first_replayed_timestamp_ns, Some(ts1));
    assert_eq!(report.last_replayed_timestamp_ns, Some(ts2));
    assert_eq!(engine.stats().memtable_entry_count, 2);
}

#[test]
fn test_recovery_skips_checkpointed_entries() {
    let dir = tempdir().unwrap();
    let ts3;

    {
        let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();
        engine.append(b"entry1").unwrap();
        engine.append(b"entry2").unwrap();
        engine.flush().unwrap();
        ts3 = engine.append(b"entry3").unwrap();
    }

    let (engine, report) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    assert!(report.checkpoint_timestamp_ns > 0);
    assert_eq!(report.wal_entries_replayed, 1);
    assert_eq!(report.first_replayed_timestamp_ns, Some(ts3));
    assert_eq!(engine.stats().memtable_entry_count, 1);
    assert_eq!(engine.stats().sstable_count, 1);
}

#[test]
fn test_auto_flush_on_threshold() {
    let dir = tempdir().unwrap();
    let config = Config { memtable_flush_threshold: 100, ..Config::default() };
    let (engine, _) = StorageEngine::open(dir.path(), config).unwrap();

    for i in 0..20 {
        engine.append(&[i; 50]).unwrap();
    }

    assert!(engine.stats().sstable_count > 0);
}

#[test]
fn test_multiple_flushes() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    engine.append(b"batch1").unwrap();
    engine.flush().unwrap();

    engine.append(b"batch2").unwrap();
    engine.flush().unwrap();

    assert_eq!(engine.stats().sstable_count, 2);
    assert_eq!(engine.sstable_count(), 2);
}

#[test]
fn test_stats_reflect_state() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    let initial = engine.stats();
    assert_eq!(initial.memtable_entry_count, 0);
    assert_eq!(initial.sstable_count, 0);
    assert_eq!(initial.checkpoint_timestamp_ns, 0);

    engine.append(b"data").unwrap();
    let after_append = engine.stats();
    assert_eq!(after_append.memtable_entry_count, 1);

    engine.flush().unwrap();
    let after_flush = engine.stats();
    assert_eq!(after_flush.memtable_entry_count, 0);
    assert_eq!(after_flush.sstable_count, 1);
    assert!(after_flush.checkpoint_timestamp_ns > 0);
}

#[test]
fn test_temp_files_cleaned_on_open() {
    let dir = tempdir().unwrap();

    fs::write(dir.path().join("orphan.sst.tmp"), b"garbage").unwrap();
    fs::write(dir.path().join("orphan.manifest.tmp"), b"garbage").unwrap();

    let (_, report) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    assert_eq!(report.temp_files_cleaned, 2);
    assert!(!dir.path().join("orphan.sst.tmp").exists());
    assert!(!dir.path().join("orphan.manifest.tmp").exists());
}

#[test]
fn test_get_returns_appended_entry_r1() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    let ts = engine.append(b"test_data").unwrap();

    let entry = engine.get(ts).unwrap();
    assert!(entry.is_some());
    let entry = entry.unwrap();
    assert_eq!(entry.timestamp_ns, ts);
    assert_eq!(&*entry.data, b"test_data");
}

#[test]
fn test_get_returns_none_for_nonexistent_timestamp() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    engine.append(b"data").unwrap();

    let entry = engine.get(999).unwrap();
    assert!(entry.is_none());
}

#[test]
fn test_get_reads_from_sstable_r1() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    let ts = engine.append(b"flushed_data").unwrap();
    engine.flush().unwrap();

    let entry = engine.get(ts).unwrap();
    assert!(entry.is_some());
    assert_eq!(&*entry.unwrap().data, b"flushed_data");
}

#[test]
fn test_entries_merged_across_flush_r2() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    engine.append(b"batch1_entry1").unwrap();
    engine.append(b"batch1_entry2").unwrap();
    engine.flush().unwrap();

    engine.append(b"batch2_entry1").unwrap();
    engine.append(b"batch2_entry2").unwrap();
    engine.flush().unwrap();

    engine.append(b"memtable_entry").unwrap();

    let entries: Vec<_> = engine.entries().unwrap().collect();
    assert_eq!(entries.len(), 5);

    for window in entries.windows(2) {
        assert!(window[0].timestamp_ns < window[1].timestamp_ns);
    }
}

#[test]
fn test_snapshot_isolation_r3() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    engine.append(b"before_snapshot").unwrap();

    let snapshot = engine.snapshot();

    engine.append(b"after_snapshot").unwrap();

    let snapshot_entries: Vec<_> = snapshot.merge_iter().unwrap().collect();
    assert_eq!(snapshot_entries.len(), 1);
    assert_eq!(&*snapshot_entries[0].data, b"before_snapshot");
}

#[test]
fn test_scan_across_sstable_and_memtable() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    engine.append(b"in_sstable").unwrap();
    engine.append(b"in_sstable").unwrap();
    engine.flush().unwrap();

    engine.append(b"in_memtable").unwrap();
    engine.append(b"in_memtable").unwrap();

    let ts: Vec<_> = engine.entries().unwrap().map(|e| e.timestamp_ns).collect();
    let scanned = engine.scan(ts[1], ts[2]).unwrap();
    assert_eq!(scanned.len(), 2);
    assert_eq!(scanned[0].timestamp_ns, ts[1]);
    assert_eq!(scanned[1].timestamp_ns, ts[2]);
}

#[test]
fn test_read_idempotency_r5() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    let ts = engine.append(b"data").unwrap();
    engine.flush().unwrap();

    let read1 = engine.get(ts).unwrap();
    let read2 = engine.get(ts).unwrap();
    let read3 = engine.get(ts).unwrap();

    assert_eq!(read1, read2);
    assert_eq!(read2, read3);

    let stats = engine.stats();
    assert_eq!(stats.sstable_count, 1);
}

#[test]
fn test_crash_recovery_reads_only_durable_r6() {
    let dir = tempdir().unwrap();

    {
        let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();
        engine.append(b"entry1").unwrap();
        engine.append(b"entry2").unwrap();
        engine.flush().unwrap();
        engine.append(b"entry3").unwrap();
    }

    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    let entries: Vec<_> = engine.entries().unwrap().collect();
    assert_eq!(entries.len(), 3);

    for window in entries.windows(2) {
        assert!(window[0].timestamp_ns < window[1].timestamp_ns);
    }
}

#[test]
fn test_reads_do_not_block_writes_r7() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    engine.append(b"initial").unwrap();

    let snapshot = engine.snapshot();

    let ts2 = engine.append(b"after_snapshot").unwrap();
    assert!(ts2 > 0);

    let _ = snapshot.merge_iter().unwrap().collect::<Vec<_>>();

    let ts3 = engine.append(b"after_read").unwrap();
    assert!(ts3 > ts2);
}

#[test]
fn test_no_duplicate_entries_r8() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    for i in 0..5 {
        engine.append(&[i]).unwrap();
    }
    engine.flush().unwrap();

    for i in 5..10 {
        engine.append(&[i]).unwrap();
    }

    let entries: Vec<_> = engine.entries().unwrap().collect();
    assert_eq!(entries.len(), 10);

    let timestamps: Vec<_> = entries.iter().map(|e| e.timestamp_ns).collect();
    let unique_timestamps: HashSet<_> = timestamps.iter().copied().collect();
    assert_eq!(timestamps.len(), unique_timestamps.len());
}

#[test]
fn test_empty_engine_operations() {
    let dir = tempdir().unwrap();
    let (engine, _) = StorageEngine::open(dir.path(), Config::default()).unwrap();

    assert!(engine.get(1).unwrap().is_none());

    let entries: Vec<_> = engine.entries().unwrap().collect();
    assert!(entries.is_empty());

    let scanned = engine.scan(1, u64::MAX).unwrap();
    assert!(scanned.is_empty());
}

#[test]
fn test_background_compaction_triggers_on_threshold() {
    let dir = tempdir().unwrap();
    let config =
        Config { memtable_flush_threshold: 100, compaction_threshold: 3, compaction_enabled: true };
    let (engine, _) = StorageEngine::open(dir.path(), config).unwrap();

    for batch in 0..4 {
        for i in 0..5 {
            engine.append(&[batch * 10 + i]).unwrap();
        }
        engine.flush().unwrap();
    }

    thread::sleep(time::Duration::from_millis(200));

    assert!(engine.sstable_count() < 4);
}

#[test]
fn test_compaction_disabled_preserves_sstables() {
    let dir = tempdir().unwrap();
    let config = Config {
        memtable_flush_threshold: 100,
        compaction_threshold: 2,
        compaction_enabled: false,
    };
    let (engine, _) = StorageEngine::open(dir.path(), config).unwrap();

    for batch in 0..4 {
        engine.append(&[batch]).unwrap();
        engine.flush().unwrap();
    }

    thread::sleep(time::Duration::from_millis(100));

    assert_eq!(engine.sstable_count(), 4);
}

#[test]
fn test_compaction_graceful_shutdown() {
    let dir = tempdir().unwrap();
    let config = Config::default();

    let start = time::Instant::now();
    {
        let (engine, _) = StorageEngine::open(dir.path(), config).unwrap();
        engine.append(b"data").unwrap();
    }
    let elapsed = start.elapsed();

    assert!(elapsed < time::Duration::from_secs(2));
}

#[test]
fn test_writes_not_blocked_during_compaction() {
    let dir = tempdir().unwrap();
    let config = Config {
        memtable_flush_threshold: 1000,
        compaction_threshold: 2,
        compaction_enabled: true,
    };
    let (engine, _) = StorageEngine::open(dir.path(), config).unwrap();

    for _ in 0..3 {
        engine.append(b"data").unwrap();
        engine.flush().unwrap();
    }

    let start = time::Instant::now();
    for _ in 0..10 {
        engine.append(b"concurrent_write").unwrap();
    }
    let elapsed = start.elapsed();

    // One second is generous but still validates writes aren't blocked during compaction,
    // which would take multiple seconds due to the 5-second compaction poll interval.
    assert!(elapsed < time::Duration::from_secs(1));
}

#[test]
fn test_compaction_skipped_in_emergency() {
    let dir = tempdir().unwrap();
    let config =
        Config { memtable_flush_threshold: 100, compaction_threshold: 3, compaction_enabled: true };
    let (engine, _) = StorageEngine::open(dir.path(), config).unwrap();

    engine.set_flush_mode(FlushMode::Emergency);

    for batch in 0..4 {
        for i in 0..5 {
            engine.append(&[batch * 10 + i]).unwrap();
        }
        engine.flush().unwrap();
    }

    thread::sleep(time::Duration::from_millis(200));

    assert_eq!(engine.sstable_count(), 4);

    engine.set_flush_mode(FlushMode::Normal);

    thread::sleep(time::Duration::from_millis(200));

    assert!(engine.sstable_count() < 4);
}
