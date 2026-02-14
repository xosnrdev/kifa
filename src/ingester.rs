//! Bounded channel-based data intake for the storage engine.
//!
//! The ingester bridges external data sources and the storage engine through a bounded MPSC
//! channel. It uses the standard library's `sync_channel` because async introduces unnecessary
//! complexity when the storage engine already performs synchronous I/O for durability guarantees.
//!
//! The bounded channel provides natural backpressure: when the buffer fills, `try_send` returns
//! the data to the caller rather than blocking. This design lets sources decide how to handle
//! congestion instead of stalling threads that may have other responsibilities.
//!
//! Construction returns a split pair—the [`Ingester`] consumes from the channel while
//! [`IngesterHandle`] instances produce into it. The handle is cloneable, allowing multiple
//! data sources to share the same intake pipeline. Out-of-band atomic flags enable graceful
//! shutdown and emergency flush escalation without competing for channel capacity.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::time::Duration;

use lib_kifa::engine::StorageEngine;
use lib_kifa::{FlushMode, MEBI};

const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Default)]
pub struct IngesterStats {
    pub entries_ingested: u64,
    pub entries_failed: u64,
    pub bytes_ingested: u64,
}

pub struct Ingester {
    engine: Arc<StorageEngine>,
    receiver: mpsc::Receiver<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
    flush_escalate: Arc<AtomicBool>,
    #[cfg(feature = "crash-test")]
    crash_test_mode: bool,
}

impl Ingester {
    #[must_use]
    pub fn new(
        engine: Arc<StorageEngine>,
        capacity: usize,
        #[cfg(feature = "crash-test")] crash_test_mode: bool,
    ) -> (Self, IngesterHandle) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let flush_escalate = Arc::new(AtomicBool::new(false));

        let ingester = Self {
            engine,
            receiver,
            shutdown: Arc::clone(&shutdown),
            flush_escalate: Arc::clone(&flush_escalate),
            #[cfg(feature = "crash-test")]
            crash_test_mode,
        };

        let handle = IngesterHandle { sender, shutdown, flush_escalate };

        (ingester, handle)
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_default_capacity(engine: Arc<StorageEngine>) -> (Self, IngesterHandle) {
        use lib_kifa::KIBI;

        #[cfg(feature = "crash-test")]
        return Self::new(engine, KIBI, false);
        #[cfg(not(feature = "crash-test"))]
        Self::new(engine, KIBI)
    }

    #[must_use]
    pub fn run(&self) -> IngesterStats {
        let mut stats = IngesterStats::default();

        loop {
            // The swap clears the flag atomically so emergency mode triggers exactly once per
            // escalation signal. Checked before shutdown so a late escalation request still fires.
            if self.flush_escalate.swap(false, Ordering::Relaxed) {
                self.engine.set_flush_mode(FlushMode::Emergency);
            }

            if self.shutdown.load(Ordering::Relaxed) {
                self.drain_remaining(&mut stats);
                return stats;
            }

            // The timeout allows periodic flag checks without a separate signaling mechanism.
            // Relaxed ordering suffices because the bounded poll interval guarantees flags
            // become visible within a predictable window.
            match self.receiver.recv_timeout(POLL_INTERVAL) {
                Ok(data) => {
                    self.process_entry(&data, &mut stats);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    self.drain_remaining(&mut stats);
                    return stats;
                }
            }
        }
    }

    fn process_entry(&self, data: &[u8], stats: &mut IngesterStats) {
        let len = data.len() as u64;

        match self.engine.append(data) {
            #[cfg(feature = "crash-test")]
            Ok(lsn) => {
                stats.entries_ingested += 1;
                stats.bytes_ingested += len;
                if self.crash_test_mode {
                    eprintln!("DURABLE:{lsn}");
                }
            }
            #[cfg(not(feature = "crash-test"))]
            Ok(_) => {
                stats.entries_ingested += 1;
                stats.bytes_ingested += len;
            }
            Err(e) => {
                log::error!("Append failed ({} MiB): {e}", len as usize / MEBI);
                stats.entries_failed += 1;
            }
        }
    }

    fn drain_remaining(&self, stats: &mut IngesterStats) {
        while let Ok(data) = self.receiver.try_recv() {
            self.process_entry(&data, stats);
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub enum SendError {
    Disconnected,
    Full(Vec<u8>),
}

#[derive(Clone)]
pub struct IngesterHandle {
    sender: SyncSender<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
    flush_escalate: Arc<AtomicBool>,
}

impl IngesterHandle {
    // The blocking send is test-only because production code should use try_send and
    // handle backpressure explicitly rather than stalling the calling thread.
    #[cfg(test)]
    pub fn send(&self, data: Vec<u8>) -> Result<(), SendError> {
        self.sender.send(data).map_err(|_| SendError::Disconnected)
    }

    pub fn try_send(&self, data: Vec<u8>) -> Result<(), SendError> {
        self.sender.try_send(data).map_err(|e| match e {
            TrySendError::Full(d) => SendError::Full(d),
            TrySendError::Disconnected(_) => SendError::Disconnected,
        })
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    // Test-only convenience method. Production code uses flush_escalate_flag() to obtain
    // the raw atomic, allowing signal handlers to set it without method call overhead.
    #[cfg(test)]
    pub fn escalate_flush(&self) {
        self.flush_escalate.store(true, Ordering::Relaxed);
    }

    // Direct flag access allows external systems like signal handlers or UPS daemons to
    // trigger shutdown or emergency flush without holding a reference to the full handle.
    #[must_use]
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    #[must_use]
    pub fn flush_escalate_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flush_escalate)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::thread;
    use std::time::Duration;

    use lib_kifa::engine::{Config, StorageEngine};
    use tempfile::tempdir;

    use super::*;

    fn create_test_engine(dir: &Path) -> Arc<StorageEngine> {
        let (engine, _) = StorageEngine::open(dir, Config::default()).unwrap();
        Arc::new(engine)
    }

    #[test]
    fn test_single_entry_persisted_i2() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());
        let engine_read = Arc::clone(&engine);

        let (ingester, handle) = Ingester::with_default_capacity(engine);

        let writer = thread::spawn(move || ingester.run());

        handle.send(b"test entry".to_vec()).unwrap();
        handle.shutdown();

        let stats = writer.join().unwrap();

        assert_eq!(stats.entries_ingested, 1);
        assert_eq!(stats.bytes_ingested, 10);
        assert_eq!(stats.entries_failed, 0);

        let entry = engine_read.get(1).unwrap();
        assert!(entry.is_some());
        assert_eq!(&*entry.unwrap().data, b"test entry");
    }

    #[test]
    fn test_multiple_entries_ordered_i6() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());
        let engine_read = Arc::clone(&engine);

        let (ingester, handle) = Ingester::with_default_capacity(engine);

        let writer = thread::spawn(move || ingester.run());

        for i in 0..10 {
            handle.send(format!("entry_{i}").into_bytes()).unwrap();
        }
        handle.shutdown();

        let stats = writer.join().unwrap();
        assert_eq!(stats.entries_ingested, 10);

        let entries: Vec<_> = engine_read.entries().unwrap().collect();
        assert_eq!(entries.len(), 10);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.lsn, (i + 1) as u64);
            assert_eq!(&*entry.data, format!("entry_{i}").as_bytes());
        }
    }

    #[test]
    fn test_shutdown_drains_channel_i3() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());
        let engine_read = Arc::clone(&engine);

        #[cfg(feature = "crash-test")]
        let (ingester, handle) = Ingester::new(engine, 100, false);
        #[cfg(not(feature = "crash-test"))]
        let (ingester, handle) = Ingester::new(engine, 100);

        for i in 0..50 {
            handle.try_send(format!("entry_{i}").into_bytes()).unwrap();
        }

        handle.shutdown();

        let stats = ingester.run();

        assert_eq!(stats.entries_ingested, 50);

        let entries: Vec<_> = engine_read.entries().unwrap().collect();
        assert_eq!(entries.len(), 50);
    }

    #[test]
    fn test_backpressure_blocks_sender_i4() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());

        #[cfg(feature = "crash-test")]
        let (ingester, handle) = Ingester::new(engine, 2, false);
        #[cfg(not(feature = "crash-test"))]
        let (ingester, handle) = Ingester::new(engine, 2);

        handle.try_send(b"entry_1".to_vec()).unwrap();
        handle.try_send(b"entry_2".to_vec()).unwrap();

        let result = handle.try_send(b"entry_3".to_vec());
        assert!(matches!(result, Err(SendError::Full(_))));

        let writer = thread::spawn(move || ingester.run());
        handle.shutdown();
        writer.join().unwrap();
    }

    #[test]
    fn test_channel_disconnect_graceful_exit() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());
        let engine_read = Arc::clone(&engine);

        let (ingester, handle) = Ingester::with_default_capacity(engine);

        handle.send(b"before_drop".to_vec()).unwrap();
        drop(handle);

        let stats = ingester.run();

        assert_eq!(stats.entries_ingested, 1);

        let entry = engine_read.get(1).unwrap();
        assert!(entry.is_some());
    }

    #[test]
    fn test_escalate_flush_does_not_panic() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());

        let (ingester, handle) = Ingester::with_default_capacity(engine);

        let writer = thread::spawn(move || ingester.run());

        handle.escalate_flush();
        thread::sleep(Duration::from_millis(200));

        handle.shutdown();
        writer.join().unwrap();
    }

    #[test]
    fn test_concurrent_senders_i1() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());
        let engine_read = Arc::clone(&engine);

        let (ingester, handle) = Ingester::with_default_capacity(engine);

        let writer = thread::spawn(move || ingester.run());

        let mut sender_threads = Vec::new();
        for source_id in 0..4 {
            let h = handle.clone();
            sender_threads.push(thread::spawn(move || {
                for i in 0..25 {
                    h.send(format!("src{source_id}_entry{i}").into_bytes()).unwrap();
                }
            }));
        }

        for t in sender_threads {
            t.join().unwrap();
        }
        handle.shutdown();

        let stats = writer.join().unwrap();
        assert_eq!(stats.entries_ingested, 100);

        let entries: Vec<_> = engine_read.entries().unwrap().collect();
        assert_eq!(entries.len(), 100);

        let lsns: Vec<_> = entries.iter().map(|e| e.lsn).collect();
        for i in 1..=100 {
            assert!(lsns.contains(&i), "missing lsn {i}");
        }
    }

    #[test]
    fn test_empty_shutdown() {
        let dir = tempdir().unwrap();
        let engine = create_test_engine(dir.path());

        let (ingester, handle) = Ingester::with_default_capacity(engine);

        handle.shutdown();

        let stats = ingester.run();

        assert_eq!(stats.entries_ingested, 0);
        assert_eq!(stats.entries_failed, 0);
        assert_eq!(stats.bytes_ingested, 0);
    }
}
