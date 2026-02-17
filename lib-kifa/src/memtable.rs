//! In-memory buffer for log entries awaiting flush to disk.
//!
//! Stores entries in timestamp order. A simple `Vec` suffices because entries are only appended
//! and iterated sequentially during flush.

use std::sync::Arc;

use crate::Entry;

#[derive(Default)]
pub struct Memtable {
    entries: Vec<Entry>,
    size_bytes: usize,
    last_timestamp_ns: u64,
    frozen: bool,
}

impl Memtable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, timestamp_ns: u64, data: Arc<[u8]>) {
        assert!(!self.frozen, "insert on frozen memtable");
        assert!(
            timestamp_ns > self.last_timestamp_ns,
            "timestamp_ns {timestamp_ns} must be greater than last_timestamp_ns {}",
            self.last_timestamp_ns
        );

        let entry = Entry { timestamp_ns, data };
        self.size_bytes += entry.size_bytes();
        self.last_timestamp_ns = timestamp_ns;
        self.entries.push(entry);
    }

    pub const fn freeze(&mut self) {
        self.frozen = true;
    }

    pub const fn unfreeze(&mut self) {
        self.frozen = false;
    }

    #[must_use]
    pub const fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    const fn is_frozen(&self) -> bool {
        self.frozen
    }

    #[must_use]
    pub const fn last_timestamp_ns(&self) -> u64 {
        self.last_timestamp_ns
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    #[cfg(test)]
    fn clear(&mut self) {
        assert!(!self.frozen, "clear on frozen memtable");
        self.entries.clear();
        self.size_bytes = 0;
        self.last_timestamp_ns = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_memtable_is_empty() {
        let mt = Memtable::new();
        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);
        assert_eq!(mt.size_bytes(), 0);
        assert_eq!(mt.last_timestamp_ns(), 0);
        assert!(!mt.is_frozen());
    }

    #[test]
    fn test_insert_updates_state() {
        let mut mt = Memtable::new();
        mt.insert(1000, Arc::from([0xAB; 100]));

        assert_eq!(mt.len(), 1);
        assert_eq!(mt.size_bytes(), size_of::<u64>() + 100);
        assert_eq!(mt.last_timestamp_ns(), 1000);
        assert!(!mt.is_empty());
    }

    #[test]
    fn test_insert_multiple_monotonic_timestamps() {
        let mut mt = Memtable::new();
        mt.insert(1000, Arc::from([0x01; 10]));
        mt.insert(2000, Arc::from([0x05; 20]));
        mt.insert(3000, Arc::from([0x64; 30]));

        assert_eq!(mt.len(), 3);
        assert_eq!(mt.last_timestamp_ns(), 3000);

        let expected_size = 3 * size_of::<u64>() + 10 + 20 + 30;
        assert_eq!(mt.size_bytes(), expected_size);
    }

    #[test]
    #[should_panic(expected = "timestamp_ns 1000 must be greater than last_timestamp_ns 2000")]
    fn test_insert_non_monotonic_timestamp_panics() {
        let mut mt = Memtable::new();
        mt.insert(2000, Arc::from([0x05; 10]));
        mt.insert(1000, Arc::from([0x01; 10]));
    }

    #[test]
    #[should_panic(expected = "timestamp_ns 2000 must be greater than last_timestamp_ns 2000")]
    fn test_insert_duplicate_timestamp_panics() {
        let mut mt = Memtable::new();
        mt.insert(2000, Arc::from([0x05; 10]));
        mt.insert(2000, Arc::from([0x05; 10]));
    }

    #[test]
    fn test_freeze_sets_frozen_flag() {
        let mut mt = Memtable::new();
        mt.insert(1000, Arc::from([0x01; 10]));
        mt.freeze();

        assert!(mt.is_frozen());
    }

    #[test]
    #[should_panic(expected = "insert on frozen memtable")]
    fn test_insert_on_frozen_panics() {
        let mut mt = Memtable::new();
        mt.freeze();
        mt.insert(1000, Arc::from([0x01; 10]));
    }

    #[test]
    fn test_iter_yields_timestamp_order() {
        let mut mt = Memtable::new();
        mt.insert(1000, Arc::from([0x0A]));
        mt.insert(2000, Arc::from([0x14]));
        mt.insert(3000, Arc::from([0x1E]));

        let timestamps: Vec<_> = mt.iter().map(|e| e.timestamp_ns).collect();
        assert_eq!(timestamps, vec![1000, 2000, 3000]);
    }

    #[test]
    fn test_entry_size_bytes() {
        let entry = Entry { timestamp_ns: 1000, data: Arc::from([0; 100]) };
        assert_eq!(entry.size_bytes(), size_of::<u64>() + 100);
    }

    #[test]
    fn test_clear_resets_state() {
        let mut mt = Memtable::new();
        mt.insert(1000, Arc::from([0x01; 10]));
        mt.insert(2000, Arc::from([0x02; 20]));
        mt.clear();

        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);
        assert_eq!(mt.size_bytes(), 0);
        assert_eq!(mt.last_timestamp_ns(), 0);
    }

    #[test]
    #[should_panic(expected = "clear on frozen memtable")]
    fn test_clear_on_frozen_panics() {
        let mut mt = Memtable::new();
        mt.insert(1000, Arc::from([0x01; 10]));
        mt.freeze();
        mt.clear();
    }

    #[test]
    fn test_insert_after_clear_allows_any_timestamp() {
        let mut mt = Memtable::new();
        mt.insert(100_000, Arc::from([0x64; 10]));
        mt.clear();
        mt.insert(1000, Arc::from([0x01; 10]));

        assert_eq!(mt.last_timestamp_ns(), 1000);
        assert_eq!(mt.len(), 1);
    }
}
