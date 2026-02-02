//! Crash-test harness for Kifa.
//!
//! Repeatedly starts Kifa with piped transactions, kills it mid-write with
//! SIGKILL, then verifies data integrity via the stats command. Proves that
//! fsync'd entries survive unclean shutdowns.
//!
//! **Flush Mode Behavior**
//!
//! The test defaults to `cautious` mode where every write is fsync'd and no
//! data loss is expected. In `normal` mode, gaps may occur because up to 49
//! entries can be lost on crash due to batched fsync. The test will report
//! gaps but this is expected behavior for normal mode, not a failure.

#![feature(string_from_utf8_lossy_owned)]

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command, Stdio};
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
}

const KIFA_BIN_PATH: &str = "./target/release/kifa";
const GEN_TRANSACTIONS_BIN_PATH: &str = "./target/release/examples/gen-transactions";

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
    /// Optional reason for failure if the cycle did not pass.
    reason: Option<String>,
}

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

/// Builds both `kifa` and `gen-transactions` binaries in release mode.
fn build_binaries() -> Result<()> {
    println!("Building kifa and gen-transactions...");

    let status = Command::new("cargo")
        .args(["build", "--release", "--workspace", "--all-targets"])
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

/// Spawns the test pipeline consisting of `gen-transactions` piped to `kifa` daemon.
///
/// The pipeline simulates a crash scenario where `gen-transactions` generates a stream
/// of transactions and `kifa` processes them via stdin. The pipeline will be forcefully
/// killed mid-write to test crash recovery.
///
/// Each cycle uses a unique seed for reproducibility while generating different
/// transaction streams across cycles.
fn spawn_test_pipeline(args: &Args, cycle: u64) -> Result<Child> {
    let mut gen_child = Command::new(GEN_TRANSACTIONS_BIN_PATH)
        .args(["-n", "0", "-r", &args.rate.to_string(), "-s", &cycle.to_string()])
        .stdout(Stdio::piped())
        .spawn()
        .or_else(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                anyhow::bail!("gen-transactions binary not found at {GEN_TRANSACTIONS_BIN_PATH}");
            }
            Err(err.into())
        })
        .context("failed to spawn gen-transactions process")?;

    let gen_stdout = gen_child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture gen-transactions stdout"))?;

    Command::new(KIFA_BIN_PATH)
        .args([
            "daemon",
            "--stdin",
            "-d",
            args.data_dir
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid UTF-8 in data directory path"))?,
            "--flush-mode",
            &args.flush_mode,
        ])
        .stdin(gen_stdout)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .or_else(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                anyhow::bail!("kifa binary not found at {KIFA_BIN_PATH}");
            }
            Err(err.into())
        })
        .context("failed to spawn kifa daemon process")
}

/// Kills all processes matching the given pattern in their command line.
///
/// This function enumerates all system processes and terminates those whose
/// command line contains the specified pattern. This is necessary because the
/// forceful kill can leave orphaned child processes.
fn kill_processes_by_pattern(pattern: &str) {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);

    for process in sys.processes().values() {
        let cmd_line = process.cmd().join(OsStr::new(" "));
        if cmd_line.to_string_lossy().contains(pattern) {
            let _ = process.kill();
        }
    }
}

/// Cleans up any orphaned test processes for the given cycle.
///
/// After killing the main pipeline, there may be orphaned child processes
/// that need to be cleaned up. This function waits briefly to allow the OS
/// to complete process cleanup before proceeding.
fn cleanup_orphaned_processes(data_dir: &Path, cycle: u64) {
    let kifa_pattern = format!("kifa daemon.*{}", data_dir.display());
    kill_processes_by_pattern(&kifa_pattern);

    let gen_pattern = format!("gen-transactions.*-s {cycle}");
    kill_processes_by_pattern(&gen_pattern);

    thread::sleep(Duration::from_secs(1));
}

/// Cleans up all `kifa` and `gen-transactions` processes.
///
/// This is called on program exit to ensure no test processes are left running.
/// Targeting all instances regardless of cycle or data directory.
fn cleanup_all_processes(data_dir: &Path) {
    let kifa_pattern = format!("kifa daemon.*{}", data_dir.display());
    kill_processes_by_pattern(&kifa_pattern);
    kill_processes_by_pattern("gen-transactions");
}

/// Runs the `kifa` stats command and captures its output.
///
/// The stats command triggers WAL recovery and reports the current state of the
/// database, which is used to verify that data was not corrupted by the crash.
fn run_stats_command(data_dir: &Path) -> Result<String> {
    let output = Command::new(KIFA_BIN_PATH)
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
fn verify_integrity(stats: &StatsResult, prev_entries: u64, flush_mode: FlushMode) -> CycleResult {
    let mut passed = true;
    let mut reason = None;

    if stats.gaps != GapStatus::None && flush_mode == FlushMode::Cautious
        || flush_mode == FlushMode::Emergency
    {
        passed = false;
        reason = Some(format!("gaps detected: {:?}", stats.gaps));
    }

    if stats.entries < prev_entries {
        passed = false;
        let msg = format!("entries decreased: {} < {}", stats.entries, prev_entries);
        reason = Some(msg);
    }

    CycleResult { passed, entries: stats.entries, gaps: stats.gaps.clone(), reason }
}

/// Executes a single crash cycle: spawn, kill, and verify.
///
/// The cycle runs for a random duration (1-5 seconds) for crashes to occur
/// at different points in the write/flush/compaction lifecycle, for
/// better coverage of crash scenarios.
fn run_single_cycle(args: &Args, cycle: u64, prev_entries: u64) -> Result<CycleResult> {
    let mut child = spawn_test_pipeline(args, cycle)?;
    let sleep_secs = fastrand::u64(1..=5);
    thread::sleep(Duration::from_secs(sleep_secs));

    let _ = child.kill();
    let _ = child.wait();

    cleanup_orphaned_processes(&args.data_dir, cycle);

    let stats_output = run_stats_command(&args.data_dir)?;
    let stats = parse_stats_output(&stats_output);
    let result = verify_integrity(&stats, prev_entries, args.flush_mode.parse().unwrap());

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

    for cycle in 1..=args.cycles {
        let result = run_single_cycle(args, cycle, prev_entries)?;

        if result.passed {
            println!(
                "[{:3}/{}] PASS  entries={:<8} gaps={:?}",
                cycle, args.cycles, result.entries, result.gaps
            );
            passed += 1;
        } else {
            println!(
                "[{:3}/{}] FAIL  entries={:<8} gaps={:?} - {}",
                cycle,
                args.cycles,
                result.entries,
                result.gaps,
                result.reason.unwrap_or_default()
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
    })
}

/// Prints the test configuration header.
fn print_test_header(args: &Args) {
    println!("\n=== Kifa Crash Test ===");
    println!("Flush mode: {}", args.flush_mode);
    println!("Cycles:     {}", args.cycles);
    println!("Rate:       {} txn/s", args.rate);
    println!("Data dir:   {}\n", args.data_dir.display());
}

/// Prints the summary of test results.
fn print_summary(summary: &TestSummary) {
    println!("\n=== Summary ===");
    println!("Cycles:    {}/{} passed", summary.passed, summary.total_cycles);
    println!("Entries:   {} verified", summary.final_entries);
    println!("Gaps:      {}", summary.gap_cycles);
    println!("Data loss: {}", summary.failed);
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
        let result = verify_integrity(&stats, 50, FlushMode::Cautious);
        assert!(result.passed);
        assert!(result.reason.is_none());
    }

    #[test]
    fn test_verify_integrity_fail_gaps_cautious() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::Present };
        let result = verify_integrity(&stats, 50, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.is_some());
    }

    #[test]
    fn test_verify_integrity_gaps_ok_in_normal() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::Present };
        let result = verify_integrity(&stats, 50, FlushMode::Normal);
        assert!(result.passed);
        assert!(result.reason.is_none());
    }

    #[test]
    fn test_verify_integrity_fail_decreased_entries() {
        let stats = StatsResult { entries: 30, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.unwrap().contains("entries decreased"));
    }

    #[test]
    fn test_verify_integrity_same_entries_pass() {
        let stats = StatsResult { entries: 50, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, FlushMode::Cautious);
        assert!(result.passed);
    }
}
