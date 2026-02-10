//! Read-only query and export interface for the storage engine.
//!
//! The module uses [`humantime`] for time parsing because the standard library's `Duration` lacks
//! string parsing entirely. This crate supports both relative offsets (`1h`, `30m`) and RFC3339
//! timestamps without pulling in heavier alternatives like `chrono`.
//!
//! JSON and CSV serialization uses manual byte-level escaping rather than `serde_json` to keep the
//! dependency footprint minimal. Binary data that fails UTF-8 validation appears as `\uXXXX`
//! escapes in JSON or `[hex:...]` fallback in CSV.
//!
//! Exports write to a temporary file, call `sync_all()`, then atomically rename to the final path.
//! Readers never observe partial output even if the process crashes mid-write.

use std::fmt;
use std::fs::{File, remove_file};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use lib_kifa::common::{atomic_rename, temp_path};
use lib_kifa::engine::{Config, Stats, StorageEngine};
use lib_kifa::{Entry, engine, map_err};
use memchr_rs::memchr;

#[derive(Debug)]
pub enum Error {
    Engine(engine::Error),
    Io(io::Error),
    InvalidLsnRange { from: u64, to: u64 },
    InvalidTimeRange { from_ms: u64, to_ms: u64 },
    TimeParseFailed(String),
}

map_err!(Engine, engine::Error);
map_err!(Io, io::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(e) => e.fmt(f),
            Self::Io(e) => e.fmt(f),
            Self::InvalidLsnRange { from, to } => {
                write!(f, "invalid LSN range: {from} > {to}")
            }
            Self::InvalidTimeRange { from_ms, to_ms } => {
                write!(
                    f,
                    "--from-time ({}) is after --to-time ({})",
                    format_timestamp_ms(*from_ms),
                    format_timestamp_ms(*to_ms)
                )
            }
            Self::TimeParseFailed(s) => {
                write!(
                    f,
                    "failed to parse time `{s}`: expected relative offset (1h, 30m, 2d) \
                     or timestamp (YYYY-MM-DD HH:mm:ss or YYYY-MM-DDTHH:mm:ssZ)"
                )
            }
        }
    }
}

impl std::error::Error for Error {}

// Deletes the temp file on drop unless disarmed. Partial writes are cleaned up if an error
// occurs before the atomic rename completes.
struct TempFileGuard<'a> {
    path: &'a Path,
    disarm: bool,
}

impl<'a> TempFileGuard<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path, disarm: false }
    }

    fn disarm(&mut self) {
        self.disarm = true;
    }
}

impl Drop for TempFileGuard<'_> {
    fn drop(&mut self) {
        if !self.disarm {
            let _ = remove_file(self.path);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
    Hex,
}

#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub data_dir: PathBuf,
    pub from_lsn: Option<u64>,
    pub to_lsn: Option<u64>,
    pub from_time_ms: Option<u64>,
    pub to_time_ms: Option<u64>,
    pub format: OutputFormat,
    pub output_file: Option<PathBuf>,
}

pub fn parse_time_input(s: &str) -> Result<u64, Error> {
    let trimmed = s.trim();

    if trimmed.is_empty() {
        return Err(Error::TimeParseFailed(s.to_string()));
    }

    if let Ok(duration) = humantime::parse_duration(trimmed) {
        let now = SystemTime::now();
        let target =
            now.checked_sub(duration).ok_or_else(|| Error::TimeParseFailed(s.to_string()))?;
        let unix_ms = target
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::TimeParseFailed(s.to_string()))?;
        return Ok(u64::try_from(unix_ms.as_millis()).unwrap_or(u64::MAX));
    }

    if let Ok(system_time) = humantime::parse_rfc3339_weak(trimmed) {
        let unix_ms = system_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::TimeParseFailed(s.to_string()))?;
        return Ok(u64::try_from(unix_ms.as_millis()).unwrap_or(u64::MAX));
    }

    // Appending midnight allows users to specify just a date like "2026-01-29" without a time component.
    let with_midnight = format!("{trimmed} 00:00:00");
    if let Ok(system_time) = humantime::parse_rfc3339_weak(&with_midnight) {
        let unix_ms = system_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::TimeParseFailed(s.to_string()))?;
        return Ok(u64::try_from(unix_ms.as_millis()).unwrap_or(u64::MAX));
    }

    Err(Error::TimeParseFailed(s.to_string()))
}

pub fn validate_time_range(from_ms: Option<u64>, to_ms: Option<u64>) -> Result<(), Error> {
    if let (Some(from_ms), Some(to_ms)) = (from_ms, to_ms)
        && from_ms > to_ms
    {
        return Err(Error::InvalidTimeRange { from_ms, to_ms });
    }
    Ok(())
}

pub fn run_query(options: &QueryOptions) -> Result<u64, Error> {
    let from = options.from_lsn.unwrap_or_default();
    let to = options.to_lsn.unwrap_or(u64::MAX);

    if from > to {
        return Err(Error::InvalidLsnRange { from, to });
    }

    let from_time = options.from_time_ms.unwrap_or_default();
    let to_time = options.to_time_ms.unwrap_or(u64::MAX);

    validate_time_range(Some(from_time), Some(to_time))?;

    // Compaction is disabled because queries are read-only and should not mutate storage.
    let config = Config { compaction_enabled: false, ..Config::default() };
    let (engine, _) = StorageEngine::open(&options.data_dir, config)?;
    let stats = engine.stats();
    let entries = engine.scan(from, to)?;
    let scan_count = entries.len();

    let mut count = 0;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    if options.format == OutputFormat::Csv {
        writeln!(writer, "lsn,timestamp,timestamp_ms,data")?;
    }

    for entry in entries {
        if entry.timestamp_ms < from_time || entry.timestamp_ms > to_time {
            continue;
        }
        write_entry(&mut writer, &entry, options.format)?;
        count += 1;
    }

    writer.flush()?;

    if count == 0 && stats.wal.last_durable_lsn > 0 {
        if scan_count == 0 {
            eprintln!(
                "No entries in LSN range. Storage has LSNs 1..={}",
                stats.wal.last_durable_lsn
            );
        } else {
            eprintln!(
                "{scan_count} entries matched the LSN range but none matched the time filter"
            );
        }
    }

    Ok(count)
}

pub fn run_export(options: &QueryOptions) -> Result<u64, Error> {
    let output_path = options.output_file.as_ref().expect("output_file required for export");
    let temp_path = temp_path(output_path);

    let from = options.from_lsn.unwrap_or_default();
    let to = options.to_lsn.unwrap_or(u64::MAX);

    if from > to {
        return Err(Error::InvalidLsnRange { from, to });
    }

    let from_time = options.from_time_ms.unwrap_or_default();
    let to_time = options.to_time_ms.unwrap_or(u64::MAX);

    validate_time_range(Some(from_time), Some(to_time))?;

    let config = Config { compaction_enabled: false, ..Config::default() };
    let (engine, _) = StorageEngine::open(&options.data_dir, config)?;
    let stats = engine.stats();
    let entries = engine.scan(from, to)?;
    let scan_count = entries.len();

    let file = File::create(&temp_path)?;
    let mut guard = TempFileGuard::new(&temp_path);
    let mut writer = BufWriter::new(file);

    if options.format == OutputFormat::Csv {
        writeln!(writer, "lsn,timestamp,timestamp_ms,data")?;
    }

    let mut count = 0;
    for entry in entries {
        if entry.timestamp_ms < from_time || entry.timestamp_ms > to_time {
            continue;
        }
        write_entry(&mut writer, &entry, options.format)?;
        count += 1;
    }

    writer.flush()?;
    let file = writer.into_inner().map_err(|e| io::Error::other(e.to_string()))?;
    file.sync_all()?;
    // The file handle must be closed before rename because Windows holds an exclusive lock.
    drop(file);

    atomic_rename(&temp_path, output_path)?;
    // Disarm after rename succeeds because the temp file no longer exists at its original path.
    guard.disarm();

    log::info!("Exported {count} entries to {}", output_path.display());

    if count == 0 && stats.wal.last_durable_lsn > 0 {
        if scan_count == 0 {
            eprintln!(
                "No entries in LSN range. Storage has LSNs 1..={}",
                stats.wal.last_durable_lsn
            );
        } else {
            eprintln!(
                "{scan_count} entries matched the LSN range but none matched the time filter"
            );
        }
    }

    Ok(count)
}

pub fn run_stats(data_dir: &Path) -> Result<Stats, Error> {
    let config = Config { compaction_enabled: false, ..Config::default() };
    let (engine, report) = StorageEngine::open(data_dir, config)?;
    let stats = engine.stats();

    let total_entries = stats.wal.last_durable_lsn;

    println!("Storage:");
    println!("  Total entries: {total_entries}");
    println!("  SSTables: {} (checkpoint LSN: {})", stats.sstable_count, stats.checkpoint_lsn);
    println!("  Memtable: {} entries", stats.memtable_entry_count);
    println!();
    println!("WAL:");
    println!("  Flush mode: {:?}", stats.wal.flush_mode);
    println!();
    println!("Health:");
    if let (Some(first_ts), Some(last_ts)) = (report.first_timestamp_ms, report.last_timestamp_ms) {
        println!(
            "  Time range: {} - {}",
            format_timestamp_ms(first_ts),
            format_timestamp_ms(last_ts)
        );
    }
    if report.gaps.is_empty() {
        println!("  Gaps: none");
    } else {
        println!("  Gaps: {} ranges", report.gaps.len());
    }

    Ok(stats)
}

fn write_entry<W: Write>(writer: &mut W, entry: &Entry, format: OutputFormat) -> io::Result<()> {
    match format {
        OutputFormat::Text => write_entry_text(writer, entry),
        OutputFormat::Json => write_entry_json(writer, entry),
        OutputFormat::Csv => write_entry_csv(writer, entry),
        OutputFormat::Hex => write_entry_hex(writer, entry),
    }
}

// Howard Hinnant's civil_from_days algorithm for zero-alloc date conversion.
// See https://howardhinnant.github.io/date_algorithms.html#civil_from_days
#[allow(clippy::cast_sign_loss)]
fn timestamp_buf(ms: u64) -> [u8; 23] {
    const MILLIS_PER_SEC: u64 = 1000;
    const SECS_PER_MIN: u32 = 60;
    const SECS_PER_HOUR: u32 = 3600;
    const SECS_PER_DAY: u64 = 86400;
    const DAYS_PER_ERA: i64 = 146_097;

    let total_secs = ms / MILLIS_PER_SEC;
    let secs_in_day = (total_secs % SECS_PER_DAY) as u32;
    let days = (total_secs / SECS_PER_DAY).cast_signed();

    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - (DAYS_PER_ERA - 1) }) / DAYS_PER_ERA;
    let doe = (z - era * DAYS_PER_ERA) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    let hour = secs_in_day / SECS_PER_HOUR;
    let minute = (secs_in_day % SECS_PER_HOUR) / SECS_PER_MIN;
    let second = secs_in_day % SECS_PER_MIN;

    let mut buf = *b"0000-00-00 00:00:00 UTC";
    write_dec4(&mut buf, 0, y as u32);
    write_dec2(&mut buf, 5, m);
    write_dec2(&mut buf, 8, d);
    write_dec2(&mut buf, 11, hour);
    write_dec2(&mut buf, 14, minute);
    write_dec2(&mut buf, 17, second);
    buf
}

fn write_dec2(buf: &mut [u8], offset: usize, val: u32) {
    buf[offset] = b'0' + (val / 10) as u8;
    buf[offset + 1] = b'0' + (val % 10) as u8;
}

fn write_dec4(buf: &mut [u8], offset: usize, val: u32) {
    buf[offset] = b'0' + (val / 1000) as u8;
    buf[offset + 1] = b'0' + ((val / 100) % 10) as u8;
    buf[offset + 2] = b'0' + ((val / 10) % 10) as u8;
    buf[offset + 3] = b'0' + (val % 10) as u8;
}

// Emits two digits per division via a precomputed pair table, halving
// the number of modulo operations compared to one-digit-at-a-time.
fn write_u64_decimal<W: Write>(writer: &mut W, value: u64) -> io::Result<()> {
    const PAIRS: &[u8; 200] = b"\
        00010203040506070809\
        10111213141516171819\
        20212223242526272829\
        30313233343536373839\
        40414243444546474849\
        50515253545556575859\
        60616263646566676869\
        70717273747576777879\
        80818283848586878889\
        90919293949596979899";
    let mut buf = [0; 20];
    let mut pos = buf.len();
    let mut n = value;
    while n >= 100 {
        let rem = (n % 100) as usize * 2;
        n /= 100;
        pos -= 2;
        buf[pos] = PAIRS[rem];
        buf[pos + 1] = PAIRS[rem + 1];
    }
    if n >= 10 {
        let rem = n as usize * 2;
        pos -= 2;
        buf[pos] = PAIRS[rem];
        buf[pos + 1] = PAIRS[rem + 1];
    } else {
        pos -= 1;
        buf[pos] = b'0' + n as u8;
    }
    writer.write_all(&buf[pos..])
}

pub struct TimestampDisplay([u8; 23]);

impl fmt::Display for TimestampDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SAFETY: `timestamp_buf` writes only ASCII digits, hyphens, colons, spaces, and "UTC".
        let s = unsafe { str::from_utf8_unchecked(&self.0) };
        f.write_str(s)
    }
}

pub fn format_timestamp_ms(ms: u64) -> TimestampDisplay {
    TimestampDisplay(timestamp_buf(ms))
}

fn write_entry_text<W: Write>(writer: &mut W, entry: &Entry) -> io::Result<()> {
    let ts = timestamp_buf(entry.timestamp_ms);
    writer.write_all(b"[")?;
    write_u64_decimal(writer, entry.lsn)?;
    writer.write_all(b"] ")?;
    writer.write_all(&ts)?;
    writer.write_all(b" ")?;
    write_lossy(writer, &entry.data)?;
    writer.write_all(b"\n")
}

fn write_lossy<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    // Validates once up front so valid UTF-8 (the common case) bypasses
    // the chunk iterator, which allocates a Utf8Chunks state machine.
    if str::from_utf8(data).is_ok() {
        return writer.write_all(data);
    }
    for chunk in data.utf8_chunks() {
        writer.write_all(chunk.valid().as_bytes())?;
        if !chunk.invalid().is_empty() {
            writer.write_all("\u{FFFD}".as_bytes())?;
        }
    }
    Ok(())
}

fn write_entry_json<W: Write>(writer: &mut W, entry: &Entry) -> io::Result<()> {
    let ts = timestamp_buf(entry.timestamp_ms);
    writer.write_all(br#"{"lsn":"#)?;
    write_u64_decimal(writer, entry.lsn)?;
    writer.write_all(br#","timestamp":""#)?;
    writer.write_all(&ts)?;
    writer.write_all(br#"","timestamp_ms":"#)?;
    write_u64_decimal(writer, entry.timestamp_ms)?;
    writer.write_all(br#","data":""#)?;
    write_json_escaped_bytes(writer, &entry.data)?;
    writer.write_all(b"\"}\n")
}

fn write_entry_csv<W: Write>(writer: &mut W, entry: &Entry) -> io::Result<()> {
    let ts = timestamp_buf(entry.timestamp_ms);
    write_u64_decimal(writer, entry.lsn)?;
    writer.write_all(br#",""#)?;
    writer.write_all(&ts)?;
    writer.write_all(br#"","#)?;
    write_u64_decimal(writer, entry.timestamp_ms)?;
    writer.write_all(br#",""#)?;
    write_csv_escaped_bytes(writer, &entry.data)?;
    writer.write_all(b"\"\n")
}

fn write_entry_hex<W: Write>(writer: &mut W, entry: &Entry) -> io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut header = [0; 34];
    write_hex16(&mut header, 0, entry.lsn);
    header[16] = b' ';
    write_hex16(&mut header, 17, entry.timestamp_ms);
    header[33] = b' ';
    writer.write_all(&header)?;

    // A stack buffer amortizes write syscalls. Each input byte yields two hex
    // characters, so 512 input bytes fill the 1024-byte buffer.
    let mut buf = [0; 1024];
    let mut pos = 0;

    for &byte in &*entry.data {
        buf[pos] = HEX[(byte >> 4) as usize];
        buf[pos + 1] = HEX[(byte & 0x0f) as usize];
        pos += 2;

        if pos == buf.len() {
            writer.write_all(&buf)?;
            pos = 0;
        }
    }

    if pos > 0 {
        writer.write_all(&buf[..pos])?;
    }

    writer.write_all(b"\n")
}

fn write_hex16(buf: &mut [u8], offset: usize, val: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = val.to_be_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[offset + i * 2] = HEX[(b >> 4) as usize];
        buf[offset + i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }
}

fn write_json_escaped_bytes<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    static SAFE: [bool; 256] = {
        let mut t = [false; 256];
        let mut b = 0x20u8;
        while b <= 0x7E {
            t[b as usize] = true;
            b += 1;
        }
        t[b'"' as usize] = false;
        t[b'\\' as usize] = false;
        t
    };

    let mut start = 0;
    let len = data.len();
    let mut i = 0;
    while i < len {
        if SAFE[data[i] as usize] {
            i += 1;
            continue;
        }
        if start < i {
            writer.write_all(&data[start..i])?;
        }
        let byte = data[i];
        match byte {
            b'"' => writer.write_all(br#"\""#)?,
            b'\\' => writer.write_all(br"\\")?,
            b'\n' => writer.write_all(br"\n")?,
            b'\r' => writer.write_all(br"\r")?,
            b'\t' => writer.write_all(br"\t")?,
            _ => {
                let hi = byte >> 4;
                let lo = byte & 0x0f;
                writer.write_all(&[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    b"0123456789abcdef"[hi as usize],
                    b"0123456789abcdef"[lo as usize],
                ])?;
            }
        }
        i += 1;
        start = i;
    }

    if start < len {
        writer.write_all(&data[start..])?;
    }

    Ok(())
}

fn write_csv_escaped_bytes<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    if str::from_utf8(data).is_ok() {
        // ASCII byte 0x22 cannot appear inside a UTF-8 multi-byte sequence
        // because continuation bytes are always in the range 0x80-0xFF. This
        // property makes a direct byte search for the quote character correct.
        let mut start = 0;
        loop {
            let pos = memchr(b'"', data, start);
            if pos < data.len() {
                // The slice includes the quote; a second quote is appended to
                // satisfy RFC 4180 doubled-quote escaping.
                writer.write_all(&data[start..=pos])?;
                writer.write_all(b"\"")?;
                start = pos + 1;
            } else {
                // The search returned the haystack length, indicating no more
                // quotes exist. The remainder is written and the loop exits.
                writer.write_all(&data[start..])?;
                break;
            }
        }
    } else {
        // The data is not valid UTF-8, so it is emitted as bracketed hex.
        // A stack buffer batches the output to reduce write call overhead.
        const HEX: &[u8; 16] = b"0123456789abcdef";
        writer.write_all(b"[hex:")?;

        let mut buf = [0; 1024];
        let mut pos = 0;
        for &byte in data {
            buf[pos] = HEX[(byte >> 4) as usize];
            buf[pos + 1] = HEX[(byte & 0x0f) as usize];
            pos += 2;
            if pos == buf.len() {
                writer.write_all(&buf)?;
                pos = 0;
            }
        }
        if pos > 0 {
            writer.write_all(&buf[..pos])?;
        }

        writer.write_all(b"]")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn now_ms() -> u64 {
        u64::try_from(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis())
            .unwrap()
    }

    fn assert_approx_eq(actual: u64, expected: u64, tolerance: u64) {
        let diff = actual.abs_diff(expected);
        assert!(diff < tolerance, "expected {expected} ± {tolerance}, got {actual} (diff: {diff})");
    }

    #[test]
    fn test_parse_time_relative_hours() {
        let result = parse_time_input("1h").unwrap();
        let expected_approx = now_ms() - 3600 * 1000;
        assert_approx_eq(result, expected_approx, 1000);
    }

    #[test]
    fn test_parse_time_relative_minutes() {
        let result = parse_time_input("30m").unwrap();
        let expected_approx = now_ms() - 30 * 60 * 1000;
        assert_approx_eq(result, expected_approx, 1000);
    }

    #[test]
    fn test_parse_time_relative_days() {
        let result = parse_time_input("2d").unwrap();
        let expected_approx = now_ms() - 2 * 24 * 3600 * 1000;
        assert_approx_eq(result, expected_approx, 1000);
    }

    #[test]
    fn test_parse_time_relative_combined() {
        let result = parse_time_input("1h30m").unwrap();
        let expected_approx = now_ms() - 90 * 60 * 1000;
        assert_approx_eq(result, expected_approx, 1000);
    }

    #[test]
    fn test_parse_time_absolute_weak() {
        let result = parse_time_input("2026-01-29 12:00:00").unwrap();
        assert_eq!(result, 1_769_688_000_000);
    }

    #[test]
    fn test_parse_time_absolute_rfc3339() {
        let result = parse_time_input("2026-01-29T12:00:00Z").unwrap();
        assert_eq!(result, 1_769_688_000_000);
    }

    #[test]
    fn test_parse_time_invalid_empty() {
        assert!(parse_time_input("").is_err());
        assert!(parse_time_input("   ").is_err());
    }

    #[test]
    fn test_parse_time_invalid_garbage() {
        assert!(parse_time_input("yesterday").is_err());
        assert!(parse_time_input("today").is_err());
    }

    #[test]
    fn test_validate_time_range_valid() {
        assert!(validate_time_range(Some(100), Some(200)).is_ok());
        assert!(validate_time_range(Some(100), Some(100)).is_ok());
        assert!(validate_time_range(None, Some(200)).is_ok());
        assert!(validate_time_range(Some(100), None).is_ok());
        assert!(validate_time_range(None, None).is_ok());
    }

    #[test]
    fn test_validate_time_range_invalid() {
        assert!(validate_time_range(Some(200), Some(100)).is_err());
    }

    #[test]
    fn test_format_json_utf8() {
        let entry = Entry {
            lsn: 42,
            timestamp_ms: 1_769_688_000_000,
            data: Arc::from(b"hello world".to_vec()),
        };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        assert_eq!(
            buf,
            b"{\"lsn\":42,\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"timestamp_ms\":1769688000000,\"data\":\"hello world\"}\n"
        );
    }

    #[test]
    fn test_format_json_special_chars() {
        let entry = Entry {
            lsn: 1,
            timestamp_ms: 1_769_688_000_000,
            data: Arc::from(b"line\nwith\ttabs\"quotes\\".to_vec()),
        };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        assert_eq!(
            buf,
            b"{\"lsn\":1,\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"timestamp_ms\":1769688000000,\"data\":\"line\\nwith\\ttabs\\\"quotes\\\\\"}\n"
        );
    }

    #[test]
    fn test_format_json_binary() {
        let entry =
            Entry { lsn: 99, timestamp_ms: 1_769_688_000_000, data: Arc::from([0x00, 0xFF, 0x80]) };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        assert_eq!(
            buf,
            b"{\"lsn\":99,\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"timestamp_ms\":1769688000000,\"data\":\"\\u0000\\u00ff\\u0080\"}\n"
        );
    }

    #[test]
    fn test_format_json_control_chars() {
        let entry =
            Entry { lsn: 7, timestamp_ms: 1_769_688_000_000, data: Arc::from([0x01, 0x1F, 0x7F]) };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        assert_eq!(
            buf,
            b"{\"lsn\":7,\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"timestamp_ms\":1769688000000,\"data\":\"\\u0001\\u001f\\u007f\"}\n"
        );
    }

    #[test]
    fn test_format_csv_utf8() {
        let entry = Entry {
            lsn: 10,
            timestamp_ms: 1_769_688_000_000,
            data: Arc::from(b"simple data".to_vec()),
        };
        let mut buf = Vec::new();
        write_entry_csv(&mut buf, &entry).unwrap();
        assert_eq!(buf, b"10,\"2026-01-29 12:00:00 UTC\",1769688000000,\"simple data\"\n");
    }

    #[test]
    fn test_format_csv_quotes() {
        let entry = Entry {
            lsn: 11,
            timestamp_ms: 1_769_688_000_000,
            data: Arc::from(b"has \"quotes\" inside".to_vec()),
        };
        let mut buf = Vec::new();
        write_entry_csv(&mut buf, &entry).unwrap();
        assert_eq!(
            buf,
            b"11,\"2026-01-29 12:00:00 UTC\",1769688000000,\"has \"\"quotes\"\" inside\"\n"
        );
    }

    #[test]
    fn test_format_csv_binary() {
        let entry = Entry {
            lsn: 12,
            timestamp_ms: 1_769_688_000_000,
            data: Arc::from([0xDE, 0xAD, 0xBE, 0xEF]),
        };
        let mut buf = Vec::new();
        write_entry_csv(&mut buf, &entry).unwrap();
        assert_eq!(buf, b"12,\"2026-01-29 12:00:00 UTC\",1769688000000,\"[hex:deadbeef]\"\n");
    }

    #[test]
    fn test_format_text() {
        let entry = Entry {
            lsn: 5,
            timestamp_ms: 1_769_688_000_000,
            data: Arc::from(b"log message".to_vec()),
        };
        let mut buf = Vec::new();
        write_entry_text(&mut buf, &entry).unwrap();
        assert_eq!(buf, b"[5] 2026-01-29 12:00:00 UTC log message\n");
    }

    #[test]
    fn test_format_hex() {
        let entry =
            Entry { lsn: 256, timestamp_ms: 1_769_688_000_000, data: Arc::from([0xCA, 0xFE]) };
        let mut buf = Vec::new();
        write_entry_hex(&mut buf, &entry).unwrap();
        assert_eq!(buf, b"0000000000000100 0000019c099fe600 cafe\n");
    }
}
