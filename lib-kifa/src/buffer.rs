//! Sector-aligned memory buffers for direct I/O.
//!
//! Linux's `O_DIRECT` flag requires buffers aligned to the filesystem's sector size (4KiB).
//! Rust's `Vec<u8>` only guarantees 1-byte alignment, and there's no safe stdlib API for
//! custom-aligned allocations. We use `std::alloc::Layout` directly to satisfy this constraint.

use std::alloc::{self, Layout};
use std::ptr::NonNull;
use std::slice;

use crate::helpers::SECTOR_SIZE;

pub struct AlignedBuffer {
    pub ptr: NonNull<u8>,
    pub size: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize) -> Self {
        let size = size.next_multiple_of(SECTOR_SIZE);
        let layout = Layout::from_size_align(size, SECTOR_SIZE).unwrap();
        // SAFETY: Layout is valid because `size` > 0 (next_multiple_of rounds up to at least
        // `SECTOR_SIZE`) and `SECTOR_SIZE` is a power of two. The returned pointer is checked
        // for null via `NonNull::new`.
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        Self { ptr: NonNull::new(ptr).expect("allocation failed"), size }
    }

    pub const fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid, aligned to `SECTOR_SIZE`, and points to `size` bytes allocated
        // in `new()`. The lifetime of the returned slice is tied to `&self`, preventing use after
        // free. No mutable references exist because we hold `&self`.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    pub const fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is valid, aligned to `SECTOR_SIZE`, and points to `size` bytes allocated
        // in `new()`. The lifetime of the returned slice is tied to `&mut self`, ensuring
        // exclusive access and preventing aliasing.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, SECTOR_SIZE).unwrap();
        // SAFETY: `ptr` was allocated with this exact layout in `new()`. After `dealloc`, no code
        // can access the memory because `Drop` consumes `self`.
        unsafe { alloc::dealloc(self.ptr.as_ptr(), layout) };
    }
}
