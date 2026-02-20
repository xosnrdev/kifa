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
    InvalidTimeRange { from_ns: u64, to_ns: u64 },
    TimeParseFailed(String),
}

map_err!(Engine, engine::Error);
map_err!(Io, io::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(e) => e.fmt(f),
            Self::Io(e) => e.fmt(f),
            Self::InvalidTimeRange { from_ns, to_ns } => {
                write!(
                    f,
                    "--from ({}) is after --to ({})",
                    format_timestamp_ns(*from_ns),
                    format_timestamp_ns(*to_ns)
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

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
    Hex,
}

pub struct QueryOptions {
    pub data_dir: PathBuf,
    pub from_ns: Option<u64>,
    pub to_ns: Option<u64>,
    pub format: OutputFormat,
    pub output_file: Option<PathBuf>,
    pub limit: u64,
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
        let unix_ns = target
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::TimeParseFailed(s.to_string()))?;
        return Ok(u64::try_from(unix_ns.as_nanos()).unwrap_or(u64::MAX));
    }

    if let Ok(system_time) = humantime::parse_rfc3339_weak(trimmed) {
        let unix_ns = system_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::TimeParseFailed(s.to_string()))?;
        return Ok(u64::try_from(unix_ns.as_nanos()).unwrap_or(u64::MAX));
    }

    let with_midnight = format!("{trimmed} 00:00:00");
    if let Ok(system_time) = humantime::parse_rfc3339_weak(&with_midnight) {
        let unix_ns = system_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::TimeParseFailed(s.to_string()))?;
        return Ok(u64::try_from(unix_ns.as_nanos()).unwrap_or(u64::MAX));
    }

    Err(Error::TimeParseFailed(s.to_string()))
}

pub fn validate_time_range(from_ns: Option<u64>, to_ns: Option<u64>) -> Result<(), Error> {
    if let (Some(from_ns), Some(to_ns)) = (from_ns, to_ns)
        && from_ns > to_ns
    {
        return Err(Error::InvalidTimeRange { from_ns, to_ns });
    }
    Ok(())
}

pub fn run_query(options: &QueryOptions) -> Result<u64, Error> {
    let from_ns = options.from_ns.unwrap_or_default();
    let to_ns = options.to_ns.unwrap_or(u64::MAX);

    validate_time_range(Some(from_ns), Some(to_ns))?;

    // Compaction is disabled because queries are read-only and should not mutate storage.
    let config = Config { compaction_enabled: false, ..Config::default() };
    let (engine, _) = StorageEngine::open(&options.data_dir, config)?;
    let entries = engine.scan(from_ns, to_ns)?;

    let mut count = 0;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    if options.format == OutputFormat::Csv {
        writeln!(writer, "timestamp,data")?;
    }

    for entry in &entries {
        if options.limit > 0 && count >= options.limit {
            break;
        }
        write_entry(&mut writer, entry, options.format)?;
        count += 1;
    }

    writer.flush()?;

    let total_entries = entries.len() as u64;
    if options.limit > 0 && total_entries > options.limit {
        eprintln!("\n(showing {count} of {total_entries} entries, use --limit 0 for all)");
    }

    if count == 0 {
        eprintln!("No entries matched the time range");
    }

    Ok(count)
}

pub fn run_export(options: &QueryOptions) -> Result<u64, Error> {
    let output_path = options.output_file.as_ref().expect("output_file required for export");
    let temp_path = temp_path(output_path);

    let from_ns = options.from_ns.unwrap_or_default();
    let to_ns = options.to_ns.unwrap_or(u64::MAX);

    validate_time_range(Some(from_ns), Some(to_ns))?;

    let config = Config { compaction_enabled: false, ..Config::default() };
    let (engine, _) = StorageEngine::open(&options.data_dir, config)?;
    let entries = engine.scan(from_ns, to_ns)?;

    let file = File::create(&temp_path)?;
    let mut guard = TempFileGuard::new(&temp_path);
    let mut writer = BufWriter::new(file);

    if options.format == OutputFormat::Csv {
        writeln!(writer, "timestamp,data")?;
    }

    let mut count = 0;
    for entry in &entries {
        if options.limit > 0 && count >= options.limit {
            break;
        }
        write_entry(&mut writer, entry, options.format)?;
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

    let total_entries = entries.len() as u64;
    if options.limit > 0 && total_entries > options.limit {
        eprintln!("\n(exported {count} of {total_entries} entries, use --limit 0 for all)");
    }

    if count == 0 {
        eprintln!("No entries matched the time range");
    }

    Ok(count)
}

pub fn run_stats(data_dir: &Path) -> Result<Stats, Error> {
    let config = Config { compaction_enabled: false, ..Config::default() };
    let (engine, _) = StorageEngine::open(data_dir, config)?;
    let stats = engine.stats();

    let mut total_entries = 0;
    let mut first_ts = None;
    let mut last_ts = None;

    for entry in engine.entries()? {
        if first_ts.is_none() {
            first_ts = Some(entry.timestamp_ns);
        }
        last_ts = Some(entry.timestamp_ns);
        total_entries += 1;
    }

    println!("Storage:");
    println!("  Total entries: {total_entries}");
    if stats.checkpoint_timestamp_ns > 0 {
        println!(
            "  SSTables: {} (checkpoint timestamp: {})",
            stats.sstable_count,
            format_timestamp_ns(stats.checkpoint_timestamp_ns)
        );
    } else {
        println!("  SSTables: {}", stats.sstable_count);
    }
    println!("  Memtable: {} entries", stats.memtable_entry_count);
    println!();
    println!("WAL:");
    println!("  Flush mode: {:?}", stats.wal.flush_mode);
    println!();
    println!("Health:");
    if let (Some(first), Some(last)) = (first_ts, last_ts) {
        println!("  Time range: {} - {}", format_timestamp_ns(first), format_timestamp_ns(last));
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
fn timestamp_buf(ns: u64) -> [u8; 23] {
    const NANOS_PER_SEC: u64 = 1_000_000_000;
    const SECS_PER_MIN: u32 = 60;
    const SECS_PER_HOUR: u32 = 3600;
    const SECS_PER_DAY: u64 = 86400;
    const DAYS_PER_ERA: i64 = 146_097;

    let total_secs = ns / NANOS_PER_SEC;
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

pub struct TimestampDisplay([u8; 23]);

impl fmt::Display for TimestampDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SAFETY: `timestamp_buf` writes only ASCII digits, hyphens, colons, spaces, and "UTC".
        let s = unsafe { str::from_utf8_unchecked(&self.0) };
        f.write_str(s)
    }
}

pub fn format_timestamp_ns(ns: u64) -> TimestampDisplay {
    TimestampDisplay(timestamp_buf(ns))
}

fn write_entry_text<W: Write>(writer: &mut W, entry: &Entry) -> io::Result<()> {
    let ts = timestamp_buf(entry.timestamp_ns);
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
    let ts = timestamp_buf(entry.timestamp_ns);
    writer.write_all(br#"{"timestamp":""#)?;
    writer.write_all(&ts)?;
    writer.write_all(br#"","data":""#)?;
    write_json_escaped_bytes(writer, &entry.data)?;
    writer.write_all(b"\"}\n")
}

fn write_entry_csv<W: Write>(writer: &mut W, entry: &Entry) -> io::Result<()> {
    let ts = timestamp_buf(entry.timestamp_ns);
    writer.write_all(br#"""#)?;
    writer.write_all(&ts)?;
    writer.write_all(br#"",""#)?;
    write_csv_escaped_bytes(writer, &entry.data)?;
    writer.write_all(b"\"\n")
}

fn write_entry_hex<W: Write>(writer: &mut W, entry: &Entry) -> io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut header = [0; 17];
    write_hex16(&mut header, 0, entry.timestamp_ns);
    header[16] = b' ';
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

    fn now_ns() -> u64 {
        u64::try_from(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos())
            .unwrap()
    }

    fn assert_approx_eq(actual: u64, expected: u64, tolerance: u64) {
        let diff = actual.abs_diff(expected);
        assert!(
            diff < tolerance,
            "expected {expected} +/- {tolerance}, got {actual} (diff: {diff})"
        );
    }

    #[test]
    fn test_parse_time_relative_hours() {
        let result = parse_time_input("1h").unwrap();
        let expected_approx = now_ns() - 3600 * 1_000_000_000;
        assert_approx_eq(result, expected_approx, 1_000_000_000);
    }

    #[test]
    fn test_parse_time_relative_minutes() {
        let result = parse_time_input("30m").unwrap();
        let expected_approx = now_ns() - 30 * 60 * 1_000_000_000;
        assert_approx_eq(result, expected_approx, 1_000_000_000);
    }

    #[test]
    fn test_parse_time_relative_days() {
        let result = parse_time_input("2d").unwrap();
        let expected_approx = now_ns() - 2 * 24 * 3600 * 1_000_000_000;
        assert_approx_eq(result, expected_approx, 1_000_000_000);
    }

    #[test]
    fn test_parse_time_relative_combined() {
        let result = parse_time_input("1h30m").unwrap();
        let expected_approx = now_ns() - 90 * 60 * 1_000_000_000;
        assert_approx_eq(result, expected_approx, 1_000_000_000);
    }

    const TS_2026_NS: u64 = 1_769_688_000_000_000_000;

    #[test]
    fn test_parse_time_absolute_weak() {
        let result = parse_time_input("2026-01-29 12:00:00").unwrap();
        assert_eq!(result, TS_2026_NS);
    }

    #[test]
    fn test_parse_time_absolute_rfc3339() {
        let result = parse_time_input("2026-01-29T12:00:00Z").unwrap();
        assert_eq!(result, TS_2026_NS);
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
        let entry = Entry { timestamp_ns: TS_2026_NS, data: Arc::from(b"hello world".to_vec()) };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        let expected = "{\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"data\":\"hello world\"}\n";
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_format_json_special_chars() {
        let entry = Entry {
            timestamp_ns: TS_2026_NS,
            data: Arc::from(b"line\nwith\ttabs\"quotes\\".to_vec()),
        };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        let expected = "{\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"data\":\"line\\nwith\\ttabs\\\"quotes\\\\\"}\n";
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_format_json_binary() {
        let entry = Entry { timestamp_ns: TS_2026_NS, data: Arc::from([0x00, 0xFF, 0x80]) };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        let expected =
            "{\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"data\":\"\\u0000\\u00ff\\u0080\"}\n";
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_format_json_control_chars() {
        let entry = Entry { timestamp_ns: TS_2026_NS, data: Arc::from([0x01, 0x1F, 0x7F]) };
        let mut buf = Vec::new();
        write_entry_json(&mut buf, &entry).unwrap();
        let expected =
            "{\"timestamp\":\"2026-01-29 12:00:00 UTC\",\"data\":\"\\u0001\\u001f\\u007f\"}\n";
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_format_csv_utf8() {
        let entry = Entry { timestamp_ns: TS_2026_NS, data: Arc::from(b"simple data".to_vec()) };
        let mut buf = Vec::new();
        write_entry_csv(&mut buf, &entry).unwrap();
        let expected = "\"2026-01-29 12:00:00 UTC\",\"simple data\"\n";
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_format_csv_quotes() {
        let entry =
            Entry { timestamp_ns: TS_2026_NS, data: Arc::from(b"has \"quotes\" inside".to_vec()) };
        let mut buf = Vec::new();
        write_entry_csv(&mut buf, &entry).unwrap();
        let expected = "\"2026-01-29 12:00:00 UTC\",\"has \"\"quotes\"\" inside\"\n";
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_format_csv_binary() {
        let entry = Entry { timestamp_ns: TS_2026_NS, data: Arc::from([0xDE, 0xAD, 0xBE, 0xEF]) };
        let mut buf = Vec::new();
        write_entry_csv(&mut buf, &entry).unwrap();
        let expected = "\"2026-01-29 12:00:00 UTC\",\"[hex:deadbeef]\"\n";
        assert_eq!(String::from_utf8(buf).unwrap(), expected);
    }

    #[test]
    fn test_format_text() {
        let entry = Entry { timestamp_ns: TS_2026_NS, data: Arc::from(b"log message".to_vec()) };
        let mut buf = Vec::new();
        write_entry_text(&mut buf, &entry).unwrap();
        assert_eq!(buf, b"2026-01-29 12:00:00 UTC log message\n");
    }

    #[test]
    fn test_format_hex() {
        let entry = Entry { timestamp_ns: TS_2026_NS, data: Arc::from([0xCA, 0xFE]) };
        let mut buf = Vec::new();
        write_entry_hex(&mut buf, &entry).unwrap();
        let expected_hex = format!("{TS_2026_NS:016x} cafe\n");
        assert_eq!(String::from_utf8(buf).unwrap(), expected_hex);
    }
}
