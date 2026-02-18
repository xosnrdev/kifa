//! Crash-test harness for Kifa.
//!
//! Repeatedly starts Kifa with piped transactions, kills it mid-write with
//! SIGKILL, then verifies data integrity by set-membership on timestamps.
//! Proves that every entry marked DURABLE via stderr is recoverable from disk
//! after an unclean shutdown.
//!
//! **`LazyFS` Mode (Linux/Docker)**
//!
//! When `--lazyfs` is enabled, the test uses `LazyFS` FIFO commands to simulate
//! true power failure by clearing unsynced cache before killing the ingester.
//!
//! **Flush Mode Behavior**
//!
//! The test defaults to `cautious` mode where every write is fsync'd and no
//! data loss is expected. In `normal` mode, up to 49 entries can be lost on
//! crash due to batched fsync. The test will report any loss but this is
//! expected behavior for normal mode, not a failure.

use std::ffi::OsStr;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::{self, Child, ChildStderr, Command, Stdio};
use std::time::Duration;
use std::{fs, io, thread};

use anyhow::{Context, Result};
use clap::{Parser, value_parser};
use lib_kifa::FlushMode;
use lib_kifa::engine::{Config, StorageEngine};
use sysinfo::{ProcessesToUpdate, System};

fn main() -> Result<()> {
    let args = Args::parse();

    let _guard = CleanupGuard(args.data_dir.clone());
    setup_signal_handler(args.data_dir.clone())?;

    if !args.skip_build {
        build_binaries()?;
    }

    setup_data_directory(&args.data_dir, args.clean)?;

    print_test_header(&args);

    let summary = run_crash_test(&args)?;

    print_summary(&summary);

    if summary.failed > 0 {
        process::exit(1);
    }

    Ok(())
}

#[derive(Parser, Debug)]
#[command(about = "Crash-test harness for Kifa.")]
struct Args {
    /// Number of crash cycles to run.
    #[arg(short = 'n', long, default_value_t = 100, value_name = "N", value_parser = value_parser!(u64).range(1..))]
    cycles: u64,

    /// Data directory for testing.
    #[arg(short = 'd', long, default_value = "./data", value_name = "DIR")]
    data_dir: PathBuf,

    /// Transaction generation rate in transactions per second.
    #[arg(short = 'r', long, default_value_t = 500, value_name = "TPS", value_parser = value_parser!(u64).range(1..))]
    rate: u64,

    /// Flush mode to test.
    #[arg(long, default_value = "cautious", value_name = "MODE")]
    flush_mode: String,

    /// Removes data directory before starting the test.
    #[arg(long)]
    clean: bool,

    /// Skips building binaries before running the test.
    #[arg(long)]
    skip_build: bool,

    /// Enables `LazyFS` crash simulation.
    #[cfg(target_os = "linux")]
    #[arg(long)]
    lazyfs: bool,

    /// `LazyFS` root directory (where persisted data lives).
    #[cfg(target_os = "linux")]
    #[arg(long, default_value = "/data/lazyfs", value_name = "DIR")]
    lazyfs_root: PathBuf,

    /// `LazyFS` FIFO path for fault injection commands.
    #[cfg(target_os = "linux")]
    #[arg(long, default_value = "/tmp/lazyfs.fifo", value_name = "PATH")]
    lazyfs_fifo: PathBuf,

    /// `LazyFS` completion FIFO path for synchronous cache clear.
    #[cfg(target_os = "linux")]
    #[arg(long, default_value = "/tmp/lazyfs_completed.fifo", value_name = "PATH")]
    lazyfs_completed_fifo: PathBuf,

    /// Path to the kifa binary.
    #[arg(long, default_value = "./target/release/kifa", value_name = "PATH")]
    kifa_bin: PathBuf,

    /// Path to the gen-transactions binary.
    #[arg(long, default_value = "./target/release/examples/gen-transactions", value_name = "PATH")]
    gen_transactions_bin: PathBuf,
}

/// Result of a single crash cycle verification.
#[derive(Debug)]
struct CycleResult {
    passed: bool,
    recovered: u64,
    durable_captured: u64,
    missing_count: u64,
    missing_timestamps: Vec<u64>,
    reason: Option<String>,
}

/// Maximum entries allowed to be lost in Normal flush mode.
const NORMAL_MODE_MAX_LOSS: u64 = 49;

/// Summary of the entire crash test run.
struct TestSummary {
    total_cycles: u64,
    passed: u64,
    failed: u64,
    final_entries: u64,
    max_missing: u64,
    total_missing: u64,
}

/// Guard that cleans up test processes on drop.
///
/// This implements RAII cleanup to handle normal program exits and panics.
/// For Ctrl-C handling, see the `setup_signal_handler` function.
struct CleanupGuard(PathBuf);

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        cleanup_all_processes(&self.0);
    }
}

/// Builds both `kifa` and `gen-transactions` binaries in release mode with crash-test feature.
fn build_binaries() -> Result<()> {
    println!("Building kifa and gen-transactions with crash-test feature...");

    let status = Command::new("cargo")
        .args(["build", "--release", "--workspace", "--all-targets", "--features", "crash-test"])
        .status()
        .context("failed to execute cargo build")?;

    if !status.success() {
        anyhow::bail!("build failed with exit code: {:?}", status.code());
    }

    Ok(())
}

/// Sets up the data directory for crash testing.
fn setup_data_directory(data_dir: &PathBuf, clean: bool) -> Result<()> {
    if clean {
        fs::remove_dir_all(data_dir)
            .or_else(|err| if err.kind() == io::ErrorKind::NotFound { Ok(()) } else { Err(err) })?;
    }

    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create directory {}", data_dir.display()))
}

/// Handle containing both child processes for the test pipeline.
struct PipelineHandle {
    kifa_child: Child,
    gen_child: Child,
}

/// Spawns the test pipeline consisting of `gen-transactions` piped to `kifa ingest`.
///
/// The pipeline simulates a crash scenario where `gen-transactions` generates a stream
/// of transactions and `kifa` processes them via stdin. The pipeline will be forcefully
/// killed mid-write to test crash recovery.
///
/// Each cycle uses a unique seed for reproducibility while generating different
/// transaction streams across cycles.
fn spawn_test_pipeline(args: &Args, cycle: u64) -> Result<PipelineHandle> {
    let gen_bin = &args.gen_transactions_bin;
    let mut gen_child = Command::new(gen_bin)
        .args(["-n", "0", "-r", &args.rate.to_string(), "-s", &cycle.to_string()])
        .stdout(Stdio::piped())
        .spawn()
        .or_else(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                anyhow::bail!("gen-transactions binary not found at {}", gen_bin.display());
            }
            Err(err.into())
        })
        .context("failed to spawn gen-transactions process")?;

    let gen_stdout = gen_child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture gen-transactions stdout"))?;

    let kifa_bin = &args.kifa_bin;
    let kifa_child = Command::new(kifa_bin)
        .args([
            "ingest",
            "--stdin",
            "-d",
            args.data_dir
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid UTF-8 in data directory path"))?,
            "--flush-mode",
            &args.flush_mode,
            "--crash-test",
        ])
        .stdin(gen_stdout)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .or_else(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                anyhow::bail!("kifa binary not found at {}", kifa_bin.display());
            }
            Err(err.into())
        })
        .context("failed to spawn kifa ingest process")?;

    Ok(PipelineHandle { kifa_child, gen_child })
}

fn parse_ingester_stderr(stderr: ChildStderr) -> (Vec<u64>, Vec<String>) {
    let reader = io::BufReader::new(stderr);
    let mut timestamps = Vec::new();
    let mut errors = Vec::new();

    for line in reader.lines().map_while(Result::ok) {
        if let Some(ts_str) = line.strip_prefix("DURABLE:")
            && let Ok(ts) = ts_str.parse::<u64>()
        {
            timestamps.push(ts);
        } else if !line.is_empty() {
            errors.push(line);
        }
    }

    (timestamps, errors)
}

/// Refreshes the system process list once and returns the System handle.
fn refreshed_system() -> System {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);
    sys
}

/// Checks whether a process command line contains all given substrings.
fn cmd_contains_all(process: &sysinfo::Process, parts: &[&str]) -> bool {
    let cmd_line = process.cmd().join(OsStr::new(" "));
    let lossy = cmd_line.to_string_lossy();
    parts.iter().all(|part| lossy.contains(part))
}

/// Cleans up any orphaned test processes for the given cycle.
///
/// Refreshes the process list once and kills matching kifa ingest and
/// gen-transactions processes in a single pass. Waits briefly afterward
/// to allow the OS to complete process cleanup before proceeding.
fn cleanup_orphaned_processes(data_dir: &Path, cycle: u64) {
    let sys = refreshed_system();
    let data_dir = data_dir.display().to_string();
    let cycle_seed = format!("-s {cycle}");

    for process in sys.processes().values() {
        let name = process.name().to_string_lossy();
        let is_orphan_kifa =
            name.contains("kifa") && cmd_contains_all(process, &["ingest", &data_dir]);
        let is_orphan_gen =
            name.contains("gen-transactions") && cmd_contains_all(process, &[&cycle_seed]);

        if is_orphan_kifa || is_orphan_gen {
            let _ = process.kill();
        }
    }

    thread::sleep(Duration::from_secs(1));
}

/// Clears `LazyFS` cache via FIFO command.
///
/// Sends `lazyfs::clear-cache` command to discard all unsynced data,
/// simulating a true power failure. Waits for completion acknowledgment
/// if the completed FIFO exists.
///
/// Reference: <https://github.com/dsrhaslab/lazyfs#running-and-injecting-faults>
#[cfg(target_os = "linux")]
fn clear_lazyfs_cache(fifo_path: &Path, completed_fifo_path: &Path) -> Result<()> {
    use std::fs::OpenOptions;
    use std::io::{BufRead, BufReader, Write};

    let mut fifo = OpenOptions::new()
        .write(true)
        .open(fifo_path)
        .with_context(|| format!("failed to open LazyFS FIFO at {}", fifo_path.display()))?;

    fifo.write_all(b"lazyfs::clear-cache\n")
        .context("failed to write clear-cache command to LazyFS FIFO")?;

    fifo.flush().context("failed to flush LazyFS FIFO")?;

    if completed_fifo_path.exists() {
        let completed_fifo =
            OpenOptions::new().read(true).open(completed_fifo_path).with_context(|| {
                format!("failed to open LazyFS completed FIFO at {}", completed_fifo_path.display())
            })?;

        let mut reader = BufReader::new(completed_fifo);
        let mut line = String::new();

        reader
            .read_line(&mut line)
            .context("failed to read completion acknowledgment from LazyFS")?;
    } else {
        thread::sleep(Duration::from_millis(100));
    }

    Ok(())
}

/// Cleans up all `kifa` and `gen-transactions` processes.
///
/// Called on program exit to kill any remaining test processes.
/// Targets all instances regardless of cycle or data directory.
fn cleanup_all_processes(data_dir: &Path) {
    let sys = refreshed_system();
    let data_dir_str = data_dir.display().to_string();

    for process in sys.processes().values() {
        let name = process.name().to_string_lossy();
        let is_kifa =
            name.contains("kifa") && cmd_contains_all(process, &["ingest", &data_dir_str]);
        let is_gen = name.contains("gen-transactions");

        if is_kifa || is_gen {
            let _ = process.kill();
        }
    }
}

/// `MergeIter` yields entries in timestamp order; the assert guards that invariant.
fn collect_recovered_timestamps(data_dir: &Path) -> Result<Vec<u64>> {
    let config = Config { compaction_enabled: false, ..Config::default() };
    let (engine, _) =
        StorageEngine::open(data_dir, config).context("failed to open engine for recovery")?;

    let iter = engine.entries().context("failed to iterate recovered entries")?;
    let timestamps: Vec<u64> = iter.map(|entry| entry.timestamp_ns).collect();
    debug_assert!(timestamps.is_sorted(), "MergeIter violated sorted-output invariant");

    Ok(timestamps)
}

/// Invariants by flush mode:
/// * Cautious/Emergency: every DURABLE timestamp must be recovered.
/// * Normal: at most 49 DURABLE timestamps may be missing.
/// * All modes: entry count must be monotonically non-decreasing.
fn verify_integrity(
    recovered: &[u64],
    durable: &[u64],
    prev_entries: u64,
    flush_mode: FlushMode,
) -> CycleResult {
    debug_assert!(recovered.is_sorted(), "recovered timestamps must be sorted for binary search");

    let recovered_count = recovered.len() as u64;
    let durable_count = durable.len() as u64;

    let mut missing_count = 0;
    let mut missing_samples = Vec::new();
    for &ts in durable {
        if recovered.binary_search(&ts).is_err() {
            missing_count += 1;
            if missing_samples.len() < 10 {
                missing_samples.push(ts);
            }
        }
    }

    let mut passed = true;
    let mut reason = None;

    if recovered_count < prev_entries {
        passed = false;
        reason = Some(format!("entries decreased: {recovered_count} < {prev_entries}"));
    } else if flush_mode.is_durable() && missing_count > 0 {
        passed = false;
        reason = Some(format!("{missing_count} durable timestamps missing from recovered set"));
    } else if !flush_mode.is_durable() && missing_count > NORMAL_MODE_MAX_LOSS {
        passed = false;
        reason = Some(format!("{missing_count} missing > {NORMAL_MODE_MAX_LOSS}"));
    }

    CycleResult {
        passed,
        recovered: recovered_count,
        durable_captured: durable_count,
        missing_count,
        missing_timestamps: missing_samples,
        reason,
    }
}

/// Executes a single crash cycle that spawn, kill, and verify.
///
/// The cycle runs for a random duration (1-5 seconds) for crashes to occur
/// at different points in the write/flush/compaction lifecycle, for
/// better coverage of crash scenarios.
///
/// When `LazyFS` is enabled, clears the cache before killing the ingester to
/// simulate true power failure (unsynced data is discarded).
fn run_single_cycle(args: &Args, cycle: u64, prev_entries: u64) -> Result<CycleResult> {
    let mut handle = spawn_test_pipeline(args, cycle)?;
    let sleep_secs = fastrand::u64(1..=5);
    thread::sleep(Duration::from_secs(sleep_secs));

    #[cfg(target_os = "linux")]
    if args.lazyfs {
        clear_lazyfs_cache(&args.lazyfs_fifo, &args.lazyfs_completed_fifo)?;
    }

    let stderr = handle.kifa_child.stderr.take();

    let _ = handle.gen_child.kill();
    let _ = handle.kifa_child.kill();
    let _ = handle.gen_child.wait();
    let _ = handle.kifa_child.wait();

    let (durable_timestamps, errors) =
        stderr.map_or_else(|| (Vec::new(), Vec::new()), parse_ingester_stderr);

    cleanup_orphaned_processes(&args.data_dir, cycle);

    #[cfg(target_os = "linux")]
    let data_dir = if args.lazyfs { &args.lazyfs_root } else { &args.data_dir };

    #[cfg(not(target_os = "linux"))]
    let data_dir = &args.data_dir;

    let recovered_timestamps = collect_recovered_timestamps(data_dir)?;

    if recovered_timestamps.is_empty() {
        eprintln!("  engine recovered zero entries from {}", data_dir.display());
    }

    if durable_timestamps.is_empty() && !errors.is_empty() {
        eprintln!("  ingester stderr contained no DURABLE lines:");
        for line in errors.iter().take(20) {
            eprintln!("  {line}");
        }
    }

    let flush_mode: FlushMode = args.flush_mode.parse().unwrap();
    let result =
        verify_integrity(&recovered_timestamps, &durable_timestamps, prev_entries, flush_mode);

    Ok(result)
}

/// Runs the complete crash test for all configured cycles.
///
/// Each cycle is independent and uses a different random seed, for a
/// diverse set of crash scenarios, collecting summarized results.
fn run_crash_test(args: &Args) -> Result<TestSummary> {
    let mut passed = 0;
    let mut failed = 0;
    let mut prev_entries = 0;
    let mut max_missing = 0;
    let mut total_missing = 0;

    for cycle in 1..=args.cycles {
        let result = run_single_cycle(args, cycle, prev_entries)?;

        max_missing = max_missing.max(result.missing_count);
        total_missing += result.missing_count;

        if result.passed {
            println!(
                "[{:3}/{}] PASS  recovered={:<8} durable={:<8} missing={:<4}",
                cycle, args.cycles, result.recovered, result.durable_captured, result.missing_count,
            );
            passed += 1;
        } else {
            println!(
                "[{:3}/{}] FAIL  recovered={:<8} durable={:<8} missing={:<4} - {}",
                cycle,
                args.cycles,
                result.recovered,
                result.durable_captured,
                result.missing_count,
                result.reason.as_deref().unwrap_or("unknown"),
            );
            if !result.missing_timestamps.is_empty() {
                eprintln!("  missing timestamps: {:?}", result.missing_timestamps);
            }
            failed += 1;
        }

        prev_entries = result.recovered;
    }

    Ok(TestSummary {
        total_cycles: args.cycles,
        passed,
        failed,
        final_entries: prev_entries,
        max_missing,
        total_missing,
    })
}

/// Prints the test configuration header.
fn print_test_header(args: &Args) {
    println!("\n<--- Kifa Crash Test --->");
    println!("Flush mode: {}", args.flush_mode);
    println!("Cycles:     {}", args.cycles);
    println!("Rate:       {} txn/s", args.rate);
    println!("Data dir:   {}", args.data_dir.display());
    #[cfg(target_os = "linux")]
    if args.lazyfs {
        println!("LazyFS:     enabled");
        println!("  Root:     {}", args.lazyfs_root.display());
        println!("  FIFO:     {}\n", args.lazyfs_fifo.display());
    } else {
        println!("LazyFS:     disabled (SIGKILL only)\n");
    }
    #[cfg(not(target_os = "linux"))]
    println!("LazyFS:     disabled (SIGKILL only)\n");
}

/// Prints the summary of test results.
fn print_summary(summary: &TestSummary) {
    println!("\n<--- Summary --->");
    println!("Cycles:     {}/{} passed", summary.passed, summary.total_cycles);
    println!("Failed:     {}", summary.failed);
    println!("Entries:    {} verified", summary.final_entries);
    println!("Max missing: {} timestamps", summary.max_missing);
    println!("Total missing: {} timestamps", summary.total_missing);
}

/// Sets up a Ctrl-C signal handler for graceful shutdown.
///
/// The handler performs cleanup before exiting to prevent orphaned processes.
fn setup_signal_handler(data_dir: PathBuf) -> Result<()> {
    ctrlc::set_handler(move || {
        println!("\n\nReceived Ctrl-C, cleaning up...");
        cleanup_all_processes(&data_dir);
        process::exit(130);
    })
    .context("failed to set Ctrl-C handler")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_durable_recovered_passes() {
        let recovered = &[10, 20, 30, 40, 50];
        let durable = &[20, 30, 40];
        let result = verify_integrity(recovered, durable, 3, FlushMode::Cautious);
        assert!(result.passed);
        assert_eq!(result.missing_count, 0);
        assert!(result.reason.is_none());
    }

    #[test]
    fn entries_decreased_fails() {
        let recovered = &[10, 20, 30];
        let durable = &[];
        let result = verify_integrity(recovered, durable, 50, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.as_ref().unwrap().contains("entries decreased"));
    }

    #[test]
    fn entries_decreased_takes_priority_over_missing() {
        let recovered = &[10, 20];
        let durable = &[20, 30, 40];
        let result = verify_integrity(recovered, durable, 50, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.as_ref().unwrap().contains("entries decreased"));
        assert_eq!(result.missing_count, 2);
    }

    #[test]
    fn same_count_no_durable_passes() {
        let recovered = &[10, 20, 30, 40, 50];
        let durable = &[];
        let result = verify_integrity(recovered, durable, 5, FlushMode::Cautious);
        assert!(result.passed);
    }

    #[test]
    fn cautious_missing_timestamp_fails() {
        let recovered = &[10, 20, 40, 50];
        let durable = &[20, 30, 40];
        let result = verify_integrity(recovered, durable, 2, FlushMode::Cautious);
        assert!(!result.passed);
        assert_eq!(result.missing_count, 1);
        assert_eq!(result.missing_timestamps, vec![30]);
        assert!(result.reason.as_ref().unwrap().contains("missing"));
    }

    #[test]
    fn emergency_missing_timestamp_fails() {
        let recovered = &[10, 20, 40, 50];
        let durable = &[20, 30, 40];
        let result = verify_integrity(recovered, durable, 2, FlushMode::Emergency);
        assert!(!result.passed);
        assert_eq!(result.missing_count, 1);
        assert_eq!(result.missing_timestamps, vec![30]);
    }

    #[test]
    fn normal_mode_tolerates_missing_within_limit() {
        let recovered: Vec<u64> = (0..60).collect();
        let durable: Vec<u64> = (11..60).collect();
        let result = verify_integrity(&recovered, &durable, 50, FlushMode::Normal);
        assert!(result.passed);
        assert_eq!(result.missing_count, 0);
    }

    #[test]
    fn normal_mode_tolerates_missing_at_boundary() {
        let recovered: Vec<u64> = (0..60).collect();
        let durable: Vec<u64> = (11..109).collect();
        let result = verify_integrity(&recovered, &durable, 50, FlushMode::Normal);
        assert!(result.passed);
        assert_eq!(result.missing_count, 49);
    }

    #[test]
    fn normal_mode_excess_missing_fails() {
        let recovered: Vec<u64> = (0..40).collect();
        let durable: Vec<u64> = (0..100).collect();
        let result = verify_integrity(&recovered, &durable, 30, FlushMode::Normal);
        assert!(!result.passed);
        assert!(result.missing_count > NORMAL_MODE_MAX_LOSS);
    }

    #[test]
    fn extra_recovered_entries_pass() {
        let recovered = &[10, 20, 30, 40, 50, 60, 70];
        let durable = &[20, 30];
        let result = verify_integrity(recovered, durable, 5, FlushMode::Cautious);
        assert!(result.passed);
        assert_eq!(result.missing_count, 0);
    }
}
