//! In-memory buffer for log entries awaiting flush to disk.
//!
//! Unlike key-value memtables that require sorted structures for lookups, this memtable
//! stores entries in LSN order. A simple `Vec` suffices because entries are only appended
//! and iterated sequentially during flush.

use std::sync::Arc;

use crate::Entry;

#[derive(Default)]
pub struct Memtable {
    entries: Vec<Entry>,
    size_bytes: usize,
    last_lsn: u64,
    frozen: bool,
}

impl Memtable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, lsn: u64, timestamp_ms: u64, data: Arc<[u8]>) {
        assert!(!self.frozen, "insert on frozen memtable");
        assert!(lsn > self.last_lsn, "lsn {lsn} must be greater than last_lsn {}", self.last_lsn);

        let entry = Entry { lsn, timestamp_ms, data };
        self.size_bytes += entry.size_bytes();
        self.last_lsn = lsn;
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
    pub const fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    #[cfg(test)]
    fn clear(&mut self) {
        assert!(!self.frozen, "clear on frozen memtable");
        self.entries.clear();
        self.size_bytes = 0;
        self.last_lsn = 0;
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
        assert_eq!(mt.last_lsn(), 0);
        assert!(!mt.is_frozen());
    }

    #[test]
    fn test_insert_updates_state() {
        let mut mt = Memtable::new();
        mt.insert(1, 1000, Arc::from(vec![0xAB; 100]));

        assert_eq!(mt.len(), 1);
        assert_eq!(mt.size_bytes(), 2 * size_of::<u64>() + 100);
        assert_eq!(mt.last_lsn(), 1);
        assert!(!mt.is_empty());
    }

    #[test]
    fn test_insert_multiple_monotonic_lsn() {
        let mut mt = Memtable::new();
        mt.insert(1, 1000, Arc::from(vec![0x01; 10]));
        mt.insert(5, 2000, Arc::from(vec![0x05; 20]));
        mt.insert(100, 3000, Arc::from(vec![0x64; 30]));

        assert_eq!(mt.len(), 3);
        assert_eq!(mt.last_lsn(), 100);

        let expected_size = 3 * 2 * size_of::<u64>() + 10 + 20 + 30;
        assert_eq!(mt.size_bytes(), expected_size);
    }

    #[test]
    #[should_panic(expected = "lsn 1 must be greater than last_lsn 5")]
    fn test_insert_non_monotonic_lsn_panics() {
        let mut mt = Memtable::new();
        mt.insert(5, 1000, Arc::from(vec![0x05; 10]));
        mt.insert(1, 2000, Arc::from(vec![0x01; 10]));
    }

    #[test]
    #[should_panic(expected = "lsn 5 must be greater than last_lsn 5")]
    fn test_insert_duplicate_lsn_panics() {
        let mut mt = Memtable::new();
        mt.insert(5, 1000, Arc::from(vec![0x05; 10]));
        mt.insert(5, 2000, Arc::from(vec![0x05; 10]));
    }

    #[test]
    fn test_freeze_sets_frozen_flag() {
        let mut mt = Memtable::new();
        mt.insert(1, 1000, Arc::from(vec![0x01; 10]));
        mt.freeze();

        assert!(mt.is_frozen());
    }

    #[test]
    #[should_panic(expected = "insert on frozen memtable")]
    fn test_insert_on_frozen_panics() {
        let mut mt = Memtable::new();
        mt.freeze();
        mt.insert(1, 1000, Arc::from(vec![0x01; 10]));
    }

    #[test]
    fn test_iter_yields_lsn_order() {
        let mut mt = Memtable::new();
        mt.insert(10, 1000, Arc::from(vec![0x0A]));
        mt.insert(20, 2000, Arc::from(vec![0x14]));
        mt.insert(30, 3000, Arc::from(vec![0x1E]));

        let lsns: Vec<_> = mt.iter().map(|e| e.lsn).collect();
        assert_eq!(lsns, vec![10, 20, 30]);
    }

    #[test]
    fn test_entry_size_bytes() {
        let entry = Entry { lsn: 42, timestamp_ms: 1000, data: Arc::from(vec![0; 100]) };
        assert_eq!(entry.size_bytes(), 2 * size_of::<u64>() + 100);
    }

    #[test]
    fn test_clear_resets_state() {
        let mut mt = Memtable::new();
        mt.insert(1, 1000, Arc::from(vec![0x01; 10]));
        mt.insert(2, 2000, Arc::from(vec![0x02; 20]));
        mt.clear();

        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);
        assert_eq!(mt.size_bytes(), 0);
        assert_eq!(mt.last_lsn(), 0);
    }

    #[test]
    #[should_panic(expected = "clear on frozen memtable")]
    fn test_clear_on_frozen_panics() {
        let mut mt = Memtable::new();
        mt.insert(1, 1000, Arc::from(vec![0x01; 10]));
        mt.freeze();
        mt.clear();
    }

    #[test]
    fn test_insert_after_clear_allows_any_lsn() {
        let mut mt = Memtable::new();
        mt.insert(100, 1000, Arc::from(vec![0x64; 10]));
        mt.clear();
        mt.insert(1, 2000, Arc::from(vec![0x01; 10]));

        assert_eq!(mt.last_lsn(), 1);
        assert_eq!(mt.len(), 1);
    }
}
