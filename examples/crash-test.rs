//! Crash-test harness for Kifa.
//!
//! Repeatedly starts Kifa with piped transactions, kills it mid-write with
//! SIGKILL, then verifies data integrity via the stats command.
//!
//! # Test Modes
//!
//! This harness supports two testing modes:
//!
//! ## SIGKILL Mode (default)
//!
//! Tests **crash consistency** by sending SIGKILL to the daemon. This verifies
//! that recovered data is valid and non-corrupted, but does NOT test true
//! durability because the kernel page cache may flush dirty pages after process
//! termination. Use this mode for quick validation during development.
//!
//! ## `LazyFS` Mode (`--lazyfs`)
//!
//! Tests **true durability** using `LazyFS`, a FUSE filesystem that maintains its
//! own page cache. Before killing the daemon, `LazyFS` clears its cache to simulate
//! power loss—only data that reached disk via `fsync()` survives. Requires Linux
//! with FUSE 3 and `LazyFS` mounted at `/tmp/lazyfs.mnt`.
//!
//! # Flush Mode Behavior
//!
//! | Mode | SIGKILL Test | `LazyFS` Test |
//! |-----------|---------------------------|-------------------------------|
//! | Normal | May show 0 lost (false +) | Should show 1-49 lost entries |
//! | Cautious | 0 entries lost | 0 entries lost |
//! | Emergency | 0 entries lost | 0 entries lost |

#![feature(string_from_utf8_lossy_owned)]

use std::borrow::Cow;
use std::ffi::OsStr;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::{self, Child, ChildStderr, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{fs, io, thread};

use anyhow::{Context, Result};
use clap::{Parser, value_parser};
use lib_kifa::FlushMode;
use sysinfo::{ProcessesToUpdate, System};

fn main() -> Result<()> {
    let args = Args::parse();

    if args.lazyfs {
        validate_lazyfs_environment()?;
    }

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

    /// Enables `LazyFS` mode for true durability testing.
    #[arg(long)]
    lazyfs: bool,
}

const KIFA_BIN_PATH: &str = "./target/release/kifa";
const GEN_TRANSACTIONS_BIN_PATH: &str = "./target/release/examples/gen-transactions";

const LAZYFS_MOUNT: &str = "/tmp/lazyfs.mnt";
const LAZYFS_ROOT: &str = "/tmp/lazyfs.root";
const LAZYFS_FIFO: &str = "/tmp/lazyfs.fifo";
const LAZYFS_DONE_FIFO: &str = "/tmp/lazyfs.done";

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
    /// Last LSN reported as durable by `Kifa` before crash.
    last_durable_lsn: u64,
    /// Number of entries lost (durable LSN minus recovered entries delta).
    entries_lost: u64,
    /// Optional reason for failure if the cycle did not pass.
    reason: Option<Cow<'static, str>>,
}

/// Summary of the entire crash test run.
struct TestSummary {
    /// Total number of cycles that were executed.
    total_cycles: u64,
    /// Number of cycles that passed all checks.
    passed: u64,
    /// Number of cycles that failed integrity checks.
    failed: u64,
    /// Number of cycles that were inconclusive (insufficient durable LSN observed).
    inconclusive: u64,
    /// Final entry count in the database.
    final_entries: u64,
    /// Number of cycles where gaps were detected.
    gap_cycles: u64,
    /// Cumulative entries lost across all cycles.
    total_entries_lost: u64,
    /// Maximum entries lost in a single cycle.
    max_entries_lost: u64,
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

fn validate_lazyfs_environment() -> Result<()> {
    let mount = Path::new(LAZYFS_MOUNT);
    if !mount.exists() {
        anyhow::bail!("LazyFS mount point does not exist: {LAZYFS_MOUNT}");
    }

    let fifo = Path::new(LAZYFS_FIFO);
    if !fifo.exists() {
        anyhow::bail!("LazyFS FIFO does not exist: {LAZYFS_FIFO}");
    }

    Ok(())
}

fn clear_lazyfs_cache() -> Result<()> {
    use std::io::Write;

    let mut fifo = fs::OpenOptions::new()
        .write(true)
        .open(LAZYFS_FIFO)
        .context("failed to open LazyFS FIFO for writing")?;

    writeln!(fifo, "lazyfs::clear-cache")?;
    fifo.flush()?;
    drop(fifo);

    let done_path = Path::new(LAZYFS_DONE_FIFO);
    if done_path.exists() {
        let mut done = fs::File::open(done_path).context("failed to open LazyFS done FIFO")?;
        let mut buf = [0; 64];
        let _ = io::Read::read(&mut done, &mut buf)?;
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

/// Handle for the test pipeline processes.
struct PipelineHandle {
    kifa_child: Child,
    gen_child: Child,
    last_durable_lsn: Arc<AtomicU64>,
    reader_handle: Option<thread::JoinHandle<()>>,
}

fn spawn_durable_reader(stderr: ChildStderr, counter: Arc<AtomicU64>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let reader = io::BufReader::new(stderr);
        for line in reader.lines() {
            let Ok(line) = line else { break };
            if let Some(n) = line.strip_prefix("DURABLE:")
                && let Ok(lsn) = n.parse::<u64>()
            {
                counter.store(lsn, Ordering::SeqCst);
            }
        }
    })
}

fn spawn_test_pipeline(args: &Args, cycle: u64, data_dir: &Path) -> Result<PipelineHandle> {
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

    let mut kifa_child = Command::new(KIFA_BIN_PATH)
        .args([
            "daemon",
            "--stdin",
            "--crash-test",
            "-d",
            data_dir
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid UTF-8 in data directory path"))?,
            "--flush-mode",
            &args.flush_mode,
        ])
        .stdin(gen_stdout)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .or_else(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                anyhow::bail!("kifa binary not found at {KIFA_BIN_PATH}");
            }
            Err(err.into())
        })
        .context("failed to spawn kifa daemon process")?;

    let kifa_stderr =
        kifa_child.stderr.take().ok_or_else(|| anyhow::anyhow!("failed to capture kifa stderr"))?;

    let last_durable_lsn = Arc::new(AtomicU64::new(0));
    let reader_handle = spawn_durable_reader(kifa_stderr, Arc::clone(&last_durable_lsn));

    Ok(PipelineHandle {
        kifa_child,
        gen_child,
        last_durable_lsn,
        reader_handle: Some(reader_handle),
    })
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

const NORMAL_MODE_MAX_LOSS: u64 = 49;

/// Minimum durable LSN required for a valid test cycle.
///
/// If the daemon didn't report at least this many durable entries before capture,
/// the cycle is inconclusive (daemon may not have started properly).
const MIN_DURABLE_FOR_VALID_CYCLE: u64 = 10;

fn verify_integrity(
    stats: &StatsResult,
    prev_entries: u64,
    last_durable_lsn: u64,
    flush_mode: FlushMode,
) -> CycleResult {
    let mut passed = true;
    let mut reason = None;

    // If we didn't observe enough durable writes, the cycle is inconclusive.
    // This happens when the daemon didn't start fast enough or stderr wasn't flushed.
    if last_durable_lsn < MIN_DURABLE_FOR_VALID_CYCLE && stats.entries > prev_entries {
        return CycleResult {
            passed: true,
            entries: stats.entries,
            gaps: stats.gaps.clone(),
            last_durable_lsn,
            entries_lost: 0,
            reason: Some("inconclusive: insufficient durable LSN observed".into()),
        };
    }

    if stats.gaps != GapStatus::None
        && (flush_mode == FlushMode::Cautious || flush_mode == FlushMode::Emergency)
    {
        passed = false;
        reason = Some(format!("gaps detected: {:?}", stats.gaps).into());
    }

    if stats.entries < prev_entries {
        passed = false;
        if reason.is_none() {
            reason =
                Some(format!("entries decreased: {} < {}", stats.entries, prev_entries).into());
        }
    }

    let entries_lost = last_durable_lsn.saturating_sub(stats.entries);

    if passed {
        match flush_mode {
            FlushMode::Normal => {
                if entries_lost > NORMAL_MODE_MAX_LOSS {
                    passed = false;
                    reason = Some(format!(
                        "lost {entries_lost} entries exceeds Normal mode limit of {NORMAL_MODE_MAX_LOSS}"
                    ).into());
                }
            }
            FlushMode::Cautious | FlushMode::Emergency => {
                if entries_lost > 0 {
                    passed = false;
                    reason = Some(
                        format!(
                            "lost {entries_lost} entries in {flush_mode:?} mode, expected zero"
                        )
                        .into(),
                    );
                }
            }
        }
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
/// In `LazyFS` mode, clears the `LazyFS` cache before killing to simulate
/// power loss where unsynced data is lost.
fn run_single_cycle(args: &Args, cycle: u64, prev_entries: u64) -> Result<CycleResult> {
    let data_dir =
        if args.lazyfs { PathBuf::from(LAZYFS_MOUNT).join("data") } else { args.data_dir.clone() };

    if args.lazyfs {
        fs::create_dir_all(&data_dir).with_context(|| {
            format!("failed to create LazyFS data directory: {}", data_dir.display())
        })?;
    }

    let mut handle = spawn_test_pipeline(args, cycle, &data_dir)?;
    let sleep_secs = fastrand::u64(1..=5);
    thread::sleep(Duration::from_secs(sleep_secs));

    let last_durable_lsn = handle.last_durable_lsn.load(Ordering::SeqCst);

    if args.lazyfs {
        clear_lazyfs_cache().context("failed to clear LazyFS cache before kill")?;
    }

    let _ = handle.kifa_child.kill();
    let _ = handle.kifa_child.wait();
    let _ = handle.gen_child.kill();
    let _ = handle.gen_child.wait();

    if let Some(reader) = handle.reader_handle.take() {
        let _ = reader.join();
    }

    cleanup_orphaned_processes(&data_dir, cycle);

    // In LazyFS mode, read stats from the root directory (not the mount) to bypass
    // O_DIRECT/FUSE incompatibility after cache clear.
    let stats_dir =
        if args.lazyfs { PathBuf::from(LAZYFS_ROOT).join("data") } else { data_dir.clone() };
    let stats_output = run_stats_command(&stats_dir)?;
    let stats = parse_stats_output(&stats_output);
    let flush_mode: FlushMode = args.flush_mode.parse().unwrap();
    let result = verify_integrity(&stats, prev_entries, last_durable_lsn, flush_mode);

    Ok(result)
}

/// Runs the complete crash test for all configured cycles.
///
/// Each cycle is independent and uses a different random seed, for a
/// diverse set of crash scenarios, collecting summarized results.
fn run_crash_test(args: &Args) -> Result<TestSummary> {
    let mut passed = 0;
    let mut failed = 0;
    let mut inconclusive = 0;
    let mut gap_cycles = 0;
    let mut prev_entries = 0;
    let mut total_entries_lost = 0;
    let mut max_entries_lost = 0;

    for cycle in 1..=args.cycles {
        let result = run_single_cycle(args, cycle, prev_entries)?;

        total_entries_lost += result.entries_lost;
        max_entries_lost = max_entries_lost.max(result.entries_lost);

        let is_inconclusive = result.reason.as_ref().is_some_and(|r| r.starts_with("inconclusive"));

        if result.passed {
            if is_inconclusive {
                println!(
                    "[{:3}/{}] SKIP  entries={:<8} gaps={:?} durable={:<8} - {}",
                    cycle,
                    args.cycles,
                    result.entries,
                    result.gaps,
                    result.last_durable_lsn,
                    result.reason.as_deref().unwrap_or_default()
                );
                inconclusive += 1;
            } else {
                println!(
                    "[{:3}/{}] PASS  entries={:<8} gaps={:?} durable={:<8} lost={}",
                    cycle,
                    args.cycles,
                    result.entries,
                    result.gaps,
                    result.last_durable_lsn,
                    result.entries_lost
                );
                passed += 1;
            }
        } else {
            println!(
                "[{:3}/{}] FAIL  entries={:<8} gaps={:?} durable={:<8} lost={} - {}",
                cycle,
                args.cycles,
                result.entries,
                result.gaps,
                result.last_durable_lsn,
                result.entries_lost,
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
        inconclusive,
        final_entries: prev_entries,
        gap_cycles,
        total_entries_lost,
        max_entries_lost,
    })
}

/// Prints the test configuration header.
fn print_test_header(args: &Args) {
    println!("\n=== Kifa Crash Test ===");
    println!(
        "Test mode:  {}",
        if args.lazyfs {
            "LazyFS + SIGKILL (for consistency and durability)"
        } else {
            "SIGKILL (for consistency)"
        }
    );
    println!("Flush mode: {}", args.flush_mode);
    println!("Cycles:     {}", args.cycles);
    println!("Rate:       {} txn/s", args.rate);
    println!("Data dir:   {}\n", args.data_dir.display());
}

/// Prints the summary of test results.
fn print_summary(summary: &TestSummary) {
    println!("\n=== Summary ===");
    println!(
        "Cycles:       {}/{} passed ({} inconclusive)",
        summary.passed, summary.total_cycles, summary.inconclusive
    );
    println!("Entries:      {} verified", summary.final_entries);
    println!("Gaps:         {} cycles with gaps", summary.gap_cycles);
    println!("Failures:     {}", summary.failed);

    println!("\n=== Durability ===");
    println!("Total lost:   {} entries", summary.total_entries_lost);
    println!("Max per crash: {} entries", summary.max_entries_lost);
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
    fn test_verify_integrity_pass_cautious() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Cautious);
        assert!(result.passed);
        assert!(result.reason.is_none());
        assert_eq!(result.entries_lost, 0);
    }

    #[test]
    fn test_verify_integrity_fail_gaps() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::Present };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.is_some());
    }

    #[test]
    fn test_verify_integrity_gaps_ok_in_normal() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::Present };
        let result = verify_integrity(&stats, 50, 100, FlushMode::Normal);
        assert!(result.passed);
        assert!(result.reason.is_none());
    }

    #[test]
    fn test_verify_integrity_fail_decreased_entries() {
        let stats = StatsResult { entries: 30, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 30, FlushMode::Cautious);
        assert!(!result.passed);
        assert!(result.reason.unwrap().contains("entries decreased"));
    }

    #[test]
    fn test_verify_integrity_same_entries_pass() {
        let stats = StatsResult { entries: 50, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 50, FlushMode::Cautious);
        assert!(result.passed);
    }

    #[test]
    fn test_durability_loss_within_normal_bounds() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 140, FlushMode::Normal);
        assert!(result.passed);
        assert_eq!(result.entries_lost, 40);
    }

    #[test]
    fn test_durability_loss_exceeds_normal_bounds() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 200, FlushMode::Normal);
        assert!(!result.passed);
        assert_eq!(result.entries_lost, 100);
        assert!(result.reason.unwrap().contains("exceeds Normal mode limit"));
    }

    #[test]
    fn test_cautious_mode_fails_on_any_loss() {
        let stats = StatsResult { entries: 100, gaps: GapStatus::None };
        let result = verify_integrity(&stats, 50, 101, FlushMode::Cautious);
        assert!(!result.passed);
        assert_eq!(result.entries_lost, 1);
        assert!(result.reason.unwrap().contains("expected zero"));
    }
}
