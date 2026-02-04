//! Crash-test harness for Kifa.
//!
//! Repeatedly starts Kifa with piped transactions, kills it mid-write with
//! SIGKILL, then verifies data integrity via the stats command. Proves that
//! fsync'd entries survive unclean shutdowns.
//!
//! **`LazyFS` Mode (Linux/Docker)**
//!
//! When `--lazyfs` is enabled, the test uses `LazyFS` FIFO commands to simulate
//! true power failure by clearing unsynced cache before killing the daemon.
//!
//! **Flush Mode Behavior**
//!
//! The test defaults to `cautious` mode where every write is fsync'd and no
//! data loss is expected. In `normal` mode, gaps may occur because up to 49
//! entries can be lost on crash due to batched fsync. The test will report
//! gaps but this is expected behavior for normal mode, not a failure.

#![feature(string_from_utf8_lossy_owned)]

use std::ffi::OsStr;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::{self, Child, ChildStderr, Command, Stdio};
use std::time::Duration;
use std::{fs, io, thread};

use anyhow::{Context, Result};
use clap::{Parser, value_parser};
use lib_kifa::FlushMode;
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

/// Status of gaps detected in the database during verification.
#[derive(Debug, PartialEq, Clone, Default)]
enum GapStatus {
    /// No gaps were detected in the sequence.
    None,
    /// One or more gaps were detected in the sequence.
    Present,
    /// Gap status could not be determined from the output.
    #[default]
    Unknown,
}

/// Statistics extracted from the kifa stats command output.
#[derive(Debug, Default)]
struct StatsResult {
    /// Total number of entries in the database.
    entries: u64,
    /// Status of sequence gaps in the database.
    gaps: GapStatus,
}

/// Result of a single crash cycle verification.
#[derive(Debug)]
struct CycleResult {
    /// Whether the cycle passed all integrity checks.
    passed: bool,
    /// Number of entries found in the database after the cycle.
    entries: u64,
    /// Gap status detected during verification.
    gaps: GapStatus,
    /// Last LSN reported as durable before crash.
    last_durable_lsn: u64,
    /// Entries lost (durable - recovered).
    entries_lost: u64,
    /// Optional reason for failure if the cycle did not pass.
    reason: Option<String>,
}

/// Maximum entries allowed to be lost in Normal flush mode.
const NORMAL_MODE_MAX_LOSS: u64 = 49;

/// Summary of the entire crash test run.
struct TestSummary {
    /// Total number of cycles that were executed.
    total_cycles: u64,
    /// Number of cycles that passed all checks.
    passed: u64,
    /// Number of cycles that failed integrity checks.
    failed: u64,
    /// Final entry count in the database.
    final_entries: u64,
    /// Number of cycles where gaps were detected.
    gap_cycles: u64,
    /// Maximum entries lost in any single cycle.
    max_loss: u64,
    /// Total entries lost across all cycles.
    total_loss: u64,
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

/// Spawns the test pipeline consisting of `gen-transactions` piped to `kifa` daemon.
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
            "daemon",
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
        .context("failed to spawn kifa daemon process")?;

    Ok(PipelineHandle { kifa_child, gen_child })
}

/// Parses stderr output to find the last durable LSN.
fn parse_last_durable_lsn(stderr: ChildStderr) -> u64 {
    let reader = io::BufReader::new(stderr);
    let mut last_lsn = 0;

    for line in reader.lines().map_while(Result::ok) {
        if let Some(lsn_str) = line.strip_prefix("DURABLE:")
            && let Ok(lsn) = lsn_str.parse::<u64>()
        {
            last_lsn = lsn;
        }
    }

    last_lsn
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
/// Refreshes the process list once and kills matching kifa daemon and
/// gen-transactions processes in a single pass. Waits briefly afterward
/// to allow the OS to complete process cleanup before proceeding.
fn cleanup_orphaned_processes(data_dir: &Path, cycle: u64) {
    let sys = refreshed_system();
    let data_dir = data_dir.display().to_string();
    let cycle_seed = format!("-s {cycle}");

    for process in sys.processes().values() {
        let name = process.name().to_string_lossy();
        let is_orphan_kifa =
            name.contains("kifa") && cmd_contains_all(process, &["daemon", &data_dir]);
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
/// This is called on program exit to ensure no test processes are left running.
/// Targeting all instances regardless of cycle or data directory.
fn cleanup_all_processes(data_dir: &Path) {
    let sys = refreshed_system();
    let data_dir_str = data_dir.display().to_string();

    for process in sys.processes().values() {
        let name = process.name().to_string_lossy();
        let is_kifa =
            name.contains("kifa") && cmd_contains_all(process, &["daemon", &data_dir_str]);
        let is_gen = name.contains("gen-transactions");

        if is_kifa || is_gen {
            let _ = process.kill();
        }
    }
}

/// Runs the `kifa` stats command and captures its output.
///
/// The stats command triggers WAL recovery and reports the current state of the
/// database, which is used to verify that data was not corrupted by the crash.
fn run_stats_command(kifa_bin: &Path, data_dir: &Path) -> Result<String> {
    let output = Command::new(kifa_bin)
        .args([
            "stats",
            "-d",
            data_dir
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid UTF-8 in data directory path"))?,
        ])
        .output()
        .context("failed to execute kifa stats command")?;

    let mut result = String::from_utf8_lossy_owned(output.stdout);
    result.push_str(&String::from_utf8_lossy(&output.stderr));

    Ok(result)
}

/// Parses the stats command output to extract entry count and gap status.
///
/// The output format is expected to contain lines like "Total entries: 12345"
/// and "Gaps: none". If parsing fails, conservative defaults are returned to
/// avoid false positives in the test results.
fn parse_stats_output(output: &str) -> StatsResult {
    let mut result = StatsResult::default();

    for line in output.lines() {
        if line.contains("Total entries:")
            && let Some(value) = line.split_whitespace().last()
        {
            result.entries = value.parse().unwrap_or_default();
        }

        if line.contains("Gaps:")
            && let Some(value) = line.split_whitespace().last()
        {
            result.gaps = if value == "none" { GapStatus::None } else { GapStatus::Present };
        }
    }

    result
}

/// Verifies the integrity of the database after a crash cycle.
///
/// Checks critical invariants based on flush mode:
/// * Cautious/Emergency: No gaps allowed, no durable entries lost.
/// * Normal: Gaps allowed, up to 49 entries at risk due to batched fsync.
/// * All modes: Entry count must be monotonically non-decreasing.
///
/// Violations indicate corruption or unexpected data loss.
fn verify_integrity(
    stats: &StatsResult,
    prev_entries: u64,
    last_durable_lsn: u64,
    flush_mode: FlushMode,
) -> CycleResult {
    let entries_lost = last_durable_lsn.saturating_sub(stats.entries);
    let mut passed = true;
    let mut reason = None;

    if stats.gaps != GapStatus::None && flush_mode.is_durable() {
        passed = false;
        reason = Some(format!("gaps detected: {:?}", stats.gaps));
    }

    if stats.entries < prev_entries {
        passed = false;
        reason = Some(format!("entries decreased: {} < {}", stats.entries, prev_entries));
    }

    if flush_mode.is_durable() && entries_lost > 0 {
        passed = false;
        reason = Some(format!(
            "durable entries lost: {} (durable={}, recovered={})",
            entries_lost, last_durable_lsn, stats.entries
        ));
    } else if !flush_mode.is_durable() && entries_lost > NORMAL_MODE_MAX_LOSS {
        passed = false;
        reason = Some(format!(
            "excessive loss: {} > {} (durable={}, recovered={})",
            entries_lost, NORMAL_MODE_MAX_LOSS, last_durable_lsn, stats.entries
        ));
    }

    CycleResult {
        passed,
        entries: stats.entries,
        gaps: stats.gaps.clone(),
        last_durable_lsn,
        entries_lost,
        reason,
    }
}

/// Executes a single crash cycle: spawn, kill, and verify.
///
/// The cycle runs for a random duration (1-5 seconds) for crashes to occur
/// at different points in the write/flush/compaction lifecycle, for
/// better coverage of crash scenarios.
///
/// When `LazyFS` is enabled, clears the cache before killing the daemon to
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

    let last_durable_lsn = stderr.map_or(0, parse_last_durable_lsn);

    cleanup_orphaned_processes(&args.data_dir, cycle);

    #[cfg(target_os = "linux")]
    let data_dir = if args.lazyfs { &args.lazyfs_root } else { &args.data_dir };

    #[cfg(not(target_os = "linux"))]
    let data_dir = &args.data_dir;

    let stats_output = run_stats_command(&args.kifa_bin, data_dir)?;
    let stats = parse_stats_output(&stats_output);
    let result =
        verify_integrity(&stats, prev_entries, last_durable_lsn, args.flush_mode.parse().unwrap());

    Ok(result)
}

/// Runs the complete crash test for all configured cycles.
///
/// Each cycle is independent and uses a different random seed, for a
/// diverse set of crash scenarios, collecting summarized results.
fn run_crash_test(args: &Args) -> Result<TestSummary> {
    let mut passed = 0;
    let mut failed = 0;
    let mut gap_cycles = 0;
    let mut prev_entries = 0;
    let mut max_loss = 0;
    let mut total_loss = 0;

    for cycle in 1..=args.cycles {
        let result = run_single_cycle(args, cycle, prev_entries)?;

        max_loss = max_loss.max(result.entries_lost);
        total_loss += result.entries_lost;

        if result.passed {
            println!(
                "[{:3}/{}] PASS  entries={:<8} durable={:<8} lost={:<4} gaps={:?}",
                cycle,
                args.cycles,
                result.entries,
                result.last_durable_lsn,
                result.entries_lost,
                result.gaps
            );
            passed += 1;
        } else {
            println!(
                "[{:3}/{}] FAIL  entries={:<8} durable={:<8} lost={:<4} gaps={:?} - {}",
                cycle,
                args.cycles,
                result.entries,
                result.last_durable_lsn,
                result.entries_lost,
                result.gaps,
                result.reason.as_deref().unwrap_or("unknown")
            );
            failed += 1;
        }

        if result.gaps != GapStatus::None {
            gap_cycles += 1;
        }

        prev_entries = result.entries;
    }

    Ok(TestSummary {
        total_cycles: args.cycles,
        passed,
        failed,
        final_entries: prev_entries,
        gap_cycles,
        max_loss,
        total_loss,
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
    println!("Gaps:       {} cycles", summary.gap_cycles);
    println!("Max loss:   {} entries", summary.max_loss);
    println!("Total loss: {} entries", summary.total_loss);
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
    fn test_parse_stats_output_no_gaps() {
        let output = "Total entries: 12345\nGaps: none\n";
        let result = parse_stats_output(output);
        assert_eq!(result.entries, 12345);
        assert_eq!(result.gaps, GapStatus::None);
    }

    #[test]
    fn test_parse_stats_output_with_gaps() {
        let output = "Total entries: 999\nGaps: 3\n";
        let result = parse_stats_output(output);
        assert_eq!(result.entries, 999);
        assert_eq!(result.gaps, GapStatus::Present);
    }

    #[test]
    fn test_parse_stats_output_empty() {
        let output = "";
        let result = parse_stats_output(output);
        assert_eq!(result.entries, 0);
        assert_eq!(result.gaps, GapStatus::Unknown);
    }

    #[test]
    fn test_verify_integrity_pass() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Cautious);
        assert!(result.passed);
        assert!(result.reason.is_none());
        assert_eq!(result.entries_lost, 0);
    }

    #[test]
    fn test_verify_integrity_fail_gaps_cautious() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::Present };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.is_some());
    }

    #[test]
    fn test_verify_integrity_allow_gaps_normal() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::Present };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Normal);
        assert!(result.passed);
    }

    #[test]
    fn test_verify_integrity_fail_decreased_entries() {
        let stats = StatsResult { entries: 30, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 30, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.as_ref().unwrap().contains("entries decreased"));
    }

    #[test]
    fn test_verify_integrity_same_entries_pass() {
        let stats = StatsResult { entries: 50, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 50, FlushMode::Cautious);
        assert!(result.passed);
    }

    #[test]
    fn test_verify_integrity_cautious_loss_fails() {
        let stats = StatsResult { entries: 95, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Cautious);
        assert!(!result.passed);
        assert_eq!(result.entries_lost, 5);
        assert!(result.reason.as_ref().unwrap().contains("durable entries lost"));
    }

    #[test]
    fn test_verify_integrity_normal_loss_within_limit() {
        let stats = StatsResult { entries: 60, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Normal);
        assert!(result.passed);
        assert_eq!(result.entries_lost, 40);
    }

    #[test]
    fn test_verify_integrity_normal_loss_exceeds_limit() {
        let stats = StatsResult { entries: 40, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 30, 100, FlushMode::Normal);
        assert!(!result.passed);
        assert_eq!(result.entries_lost, 60);
        assert!(result.reason.as_ref().unwrap().contains("excessive loss"));
    }
}
