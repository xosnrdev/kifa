//! CLI entrypoint and command dispatcher.
//!
//! The module uses [`clap`] for argument parsing because the standard library provides only raw
//! argument iteration via `std::env::args()`. Clap handles subcommands, type validation, help
//! generation, and argument conflicts automatically.
//!
//! Signal handling relies on [`signal_hook`] since Rust has no signal API in its standard library.
//! The crate implements the self-pipe pattern required for safe signal handling. On Unix, SIGINT
//! triggers graceful shutdown, SIGTERM does the same for systemd integration, and SIGUSR1 escalates
//! flush mode. Windows supports only SIGINT due to platform limitations.
//!
//! Exit codes follow the Unix convention of 128 plus the signal number: 130 for SIGINT, 143 for
//! SIGTERM. Container orchestrators like Docker and Kubernetes use these codes to distinguish
//! graceful termination from forced kills.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, thread};

use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use clap_cargo::style::CLAP_STYLING;
use lib_kifa::FlushMode;
use lib_kifa::engine::{Config, RecoveryReport, StorageEngine};
#[cfg(windows)]
use signal_hook::consts::signal::SIGINT;
#[cfg(unix)]
use signal_hook::consts::signal::{SIGINT, SIGTERM, SIGUSR1};
use signal_hook::flag;

use crate::config::{self, AppConfig, PartialConfig};
use crate::ingester::Ingester;
use crate::query::{
    self, OutputFormat, QueryOptions, format_timestamp_ms, parse_time_input, validate_time_range,
};
use crate::source::{self, FileTailer, SourceRunner, StdinSource, TcpSource, UdpSource};

pub fn run() -> Result<ExitCode> {
    let cli = Cli::parse();
    let resolved = resolve_command(&cli)?;

    match resolved {
        ResolvedCommand::Query { data_dir, cmd } => run_query_command(&data_dir, &cmd),
        ResolvedCommand::Export { data_dir, cmd } => run_export_command(&data_dir, &cmd),
        ResolvedCommand::Stats { data_dir } => run_stats_command(&data_dir),
        ResolvedCommand::Daemon(app_config) => run_daemon(&app_config),
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum FlushModeArg {
    Normal,
    Cautious,
    Emergency,
}

impl From<FlushModeArg> for FlushMode {
    fn from(arg: FlushModeArg) -> Self {
        match arg {
            FlushModeArg::Normal => Self::Normal,
            FlushModeArg::Cautious => Self::Cautious,
            FlushModeArg::Emergency => Self::Emergency,
        }
    }
}

#[derive(Parser)]
#[command(about, version, styles = CLAP_STYLING, arg_required_else_help = true)]
struct Cli {
    #[arg(short, long, global = true, help = "Directory for WAL, SSTables, and metadata")]
    data_dir: Option<PathBuf>,

    #[arg(short, long, global = true, help = "Path to TOML configuration file")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Run as log ingestion daemon")]
    Daemon(DaemonCmd),

    #[command(about = "Query stored log entries")]
    Query(QueryCmd),

    #[command(about = "Export logs to file")]
    Export(ExportCmd),

    #[command(about = "Show storage statistics")]
    Stats,
}

#[derive(Parser)]
struct DaemonCmd {
    #[arg(long, help = "Read log lines from standard input")]
    stdin: bool,

    #[arg(long, action = ArgAction::Append, help = "Tail log file (can specify multiple)")]
    file: Vec<PathBuf>,

    #[arg(long, action = ArgAction::Append, help = "Listen on TCP address (e.g., 127.0.0.1:5000)")]
    tcp: Vec<String>,

    #[arg(long, action = ArgAction::Append, help = "Listen on UDP address (e.g., 127.0.0.1:5000)")]
    udp: Vec<String>,

    #[arg(long, value_enum, default_value = "normal", help = "WAL flush mode")]
    flush_mode: Option<FlushModeArg>,

    #[arg(long, help = "WAL segment size in bytes")]
    segment_size: Option<usize>,

    #[arg(long, help = "Flush memtable to SSTable after this many bytes")]
    memtable_threshold: Option<usize>,

    #[arg(long, conflicts_with = "no_compaction", help = "Compact after this many SSTables")]
    compaction_threshold: Option<usize>,

    #[arg(long, help = "Disable background compaction")]
    no_compaction: bool,

    #[arg(long, help = "Internal channel buffer size")]
    channel_capacity: Option<usize>,
}

#[derive(Parser, Clone)]
struct QueryCmd {
    #[arg(long, help = "Start from this log sequence number (inclusive)")]
    from_lsn: Option<u64>,

    #[arg(long, help = "End at this log sequence number (inclusive)")]
    to_lsn: Option<u64>,

    #[arg(
        long,
        help = "Filter from time: relative (1h, 30m, 2d) or absolute (YYYY-MM-DD HH:mm:ss)"
    )]
    from_time: Option<String>,

    #[arg(
        long,
        help = "Filter until time: relative (1h, 30m, 2d) or absolute (YYYY-MM-DD HH:mm:ss)"
    )]
    to_time: Option<String>,

    #[arg(short, long, value_enum, default_value = "text", help = "Output format")]
    format: OutputFormat,
}

#[derive(Parser, Clone)]
struct ExportCmd {
    #[arg(long, help = "Start from this log sequence number (inclusive)")]
    from_lsn: Option<u64>,

    #[arg(long, help = "End at this log sequence number (inclusive)")]
    to_lsn: Option<u64>,

    #[arg(
        long,
        help = "Filter from time: relative (1h, 30m, 2d) or absolute (YYYY-MM-DD HH:mm:ss)"
    )]
    from_time: Option<String>,

    #[arg(
        long,
        help = "Filter until time: relative (1h, 30m, 2d) or absolute (YYYY-MM-DD HH:mm:ss)"
    )]
    to_time: Option<String>,

    #[arg(short, long, value_enum, default_value = "json", help = "Output format")]
    format: OutputFormat,

    #[arg(short, long, help = "Output file path")]
    output: PathBuf,
}

enum ResolvedCommand {
    Daemon(AppConfig),
    Query { data_dir: PathBuf, cmd: QueryCmd },
    Export { data_dir: PathBuf, cmd: ExportCmd },
    Stats { data_dir: PathBuf },
}

fn build_partial_config(cli: &Cli, daemon: Option<&DaemonCmd>) -> PartialConfig {
    let mut partial = PartialConfig {
        data_dir: cli.data_dir.clone(),
        config_file: cli.config.clone(),
        ..PartialConfig::default()
    };

    // Flags and empty vectors map to None so the config layer treats them as unspecified.
    // This preserves three-layer precedence: CLI flags override file settings only when present.
    if let Some(d) = daemon {
        partial.memtable_flush_threshold = d.memtable_threshold;
        partial.compaction_threshold = d.compaction_threshold;
        partial.compaction_enabled = if d.no_compaction { Some(false) } else { None };
        partial.flush_mode = d.flush_mode.map(FlushMode::from);
        partial.segment_size = d.segment_size;
        partial.channel_capacity = d.channel_capacity;
        partial.stdin = if d.stdin { Some(true) } else { None };
        partial.files = if d.file.is_empty() { None } else { Some(d.file.clone()) };
        partial.tcp = if d.tcp.is_empty() { None } else { Some(d.tcp.clone()) };
        partial.udp = if d.udp.is_empty() { None } else { Some(d.udp.clone()) };
    }

    partial
}

fn resolve_command(cli: &Cli) -> Result<ResolvedCommand> {
    match &cli.command {
        Some(Command::Query(cmd)) => {
            let partial = build_partial_config(cli, None);
            let app_config = config::load(partial).context("loading config")?;
            Ok(ResolvedCommand::Query { data_dir: app_config.data_dir, cmd: cmd.clone() })
        }
        Some(Command::Export(cmd)) => {
            let partial = build_partial_config(cli, None);
            let app_config = config::load(partial).context("loading config")?;
            Ok(ResolvedCommand::Export { data_dir: app_config.data_dir, cmd: cmd.clone() })
        }
        Some(Command::Stats) => {
            let partial = build_partial_config(cli, None);
            let app_config = config::load(partial).context("loading config")?;
            Ok(ResolvedCommand::Stats { data_dir: app_config.data_dir })
        }
        Some(Command::Daemon(daemon_cmd)) => {
            let partial = build_partial_config(cli, Some(daemon_cmd));
            let app_config = config::load(partial).context("loading config")?;

            if !app_config.sources.stdin
                && app_config.sources.files.is_empty()
                && app_config.sources.tcp.is_empty()
                && app_config.sources.udp.is_empty()
            {
                bail!("at least one source required (--stdin, --file, --tcp, --udp)");
            }
            Ok(ResolvedCommand::Daemon(app_config))
        }
        // No subcommand defaults to daemon mode, allowing `kifa -c config.toml` without explicit `daemon`.
        None => {
            let default_daemon = DaemonCmd {
                stdin: false,
                file: Vec::new(),
                tcp: Vec::new(),
                udp: Vec::new(),
                flush_mode: None,
                segment_size: None,
                memtable_threshold: None,
                compaction_threshold: None,
                no_compaction: false,
                channel_capacity: None,
            };
            let partial = build_partial_config(cli, Some(&default_daemon));
            let app_config = config::load(partial).context("loading config")?;

            if !app_config.sources.stdin
                && app_config.sources.files.is_empty()
                && app_config.sources.tcp.is_empty()
                && app_config.sources.udp.is_empty()
            {
                bail!("at least one source required (--stdin, --file, --tcp, --udp)");
            }
            Ok(ResolvedCommand::Daemon(app_config))
        }
    }
}

fn print_recovery_report(report: &RecoveryReport) {
    eprintln!("Recovery: {} entries replayed", report.wal_entries_replayed);
    eprintln!("  SSTables: {}", report.sstable_count);

    if let (Some(first), Some(last)) = (report.first_timestamp_ms, report.last_timestamp_ms) {
        eprintln!("  Time range: {} - {}", format_timestamp_ms(first), format_timestamp_ms(last));
    }

    if report.orphan_sstables_cleaned > 0 {
        eprintln!("  Orphan SSTables cleaned: {}", report.orphan_sstables_cleaned);
    }

    if !report.gaps.is_empty() {
        eprintln!("  WARNING: {} gaps in LSN sequence:", report.gaps.len());
        for gap in &report.gaps {
            eprintln!("    Missing: {} - {}", gap.start, gap.end);
        }
    }
}

#[cfg(unix)]
fn register_signals(
    shutdown: &Arc<AtomicBool>,
    escalate: &Arc<AtomicBool>,
    sigint_flag: &Arc<AtomicBool>,
    sigterm_flag: &Arc<AtomicBool>,
) -> Result<(), io::Error> {
    // First SIGINT sets force_exit and shutdown flags for graceful termination.
    // Second SIGINT triggers conditional_shutdown, which exits immediately with code 130.
    let force_exit = Arc::new(AtomicBool::new(false));

    flag::register_conditional_shutdown(SIGINT, 130, Arc::clone(&force_exit))?;
    flag::register(SIGINT, Arc::clone(&force_exit))?;
    flag::register(SIGINT, Arc::clone(sigint_flag))?;
    flag::register(SIGINT, Arc::clone(shutdown))?;

    flag::register(SIGTERM, Arc::clone(sigterm_flag))?;
    flag::register(SIGTERM, Arc::clone(shutdown))?;

    flag::register(SIGUSR1, Arc::clone(escalate))?;

    Ok(())
}

#[cfg(windows)]
fn register_signals(
    shutdown: &Arc<AtomicBool>,
    _escalate: &Arc<AtomicBool>,
    sigint_flag: &Arc<AtomicBool>,
    _sigterm_flag: &Arc<AtomicBool>,
) -> Result<(), io::Error> {
    flag::register(SIGINT, Arc::clone(sigint_flag))?;
    flag::register(SIGINT, Arc::clone(shutdown))?;
    Ok(())
}

fn run_query_command(data_dir: &Path, cmd: &QueryCmd) -> Result<ExitCode> {
    let from_time_ms = cmd
        .from_time
        .as_deref()
        .map(parse_time_input)
        .transpose()
        .context("invalid --from-time")?;
    let to_time_ms =
        cmd.to_time.as_deref().map(parse_time_input).transpose().context("invalid --to-time")?;

    validate_time_range(from_time_ms, to_time_ms).context("invalid time range")?;

    let options = QueryOptions {
        data_dir: data_dir.to_path_buf(),
        from_lsn: cmd.from_lsn,
        to_lsn: cmd.to_lsn,
        from_time_ms,
        to_time_ms,
        format: cmd.format,
        output_file: None,
    };

    let count = query::run_query(&options).context("query failed")?;
    eprintln!("Returned {count} entries");
    Ok(ExitCode::SUCCESS)
}

fn run_export_command(data_dir: &Path, cmd: &ExportCmd) -> Result<ExitCode> {
    let from_time_ms = cmd
        .from_time
        .as_deref()
        .map(parse_time_input)
        .transpose()
        .context("invalid --from-time")?;
    let to_time_ms =
        cmd.to_time.as_deref().map(parse_time_input).transpose().context("invalid --to-time")?;

    validate_time_range(from_time_ms, to_time_ms).context("invalid time range")?;

    let options = QueryOptions {
        data_dir: data_dir.to_path_buf(),
        from_lsn: cmd.from_lsn,
        to_lsn: cmd.to_lsn,
        from_time_ms,
        to_time_ms,
        format: cmd.format,
        output_file: Some(cmd.output.clone()),
    };

    query::run_export(&options).context("export failed")?;
    Ok(ExitCode::SUCCESS)
}

fn run_stats_command(data_dir: &Path) -> Result<ExitCode> {
    query::run_stats(data_dir).context("stats failed")?;
    Ok(ExitCode::SUCCESS)
}

fn run_daemon(config: &AppConfig) -> Result<ExitCode> {
    let engine_config = Config {
        memtable_flush_threshold: config.storage.memtable_flush_threshold,
        compaction_threshold: config.storage.compaction_threshold,
        compaction_enabled: config.storage.compaction_enabled,
    };

    let (engine, recovery_report) = StorageEngine::open(&config.data_dir, engine_config)
        .context("failed to open storage engine")?;

    print_recovery_report(&recovery_report);

    let engine = Arc::new(engine);
    let (ingester, handle) = Ingester::new(Arc::clone(&engine), config.ingester.channel_capacity);

    engine.set_flush_mode(config.wal.flush_mode);

    let shutdown = handle.shutdown_flag();
    let escalate = handle.flush_escalate_flag();
    let sigint_flag = Arc::new(AtomicBool::new(false));
    let sigterm_flag = Arc::new(AtomicBool::new(false));

    register_signals(&shutdown, &escalate, &sigint_flag, &sigterm_flag)
        .context("failed to register signals")?;

    let mut runner = SourceRunner::new();

    if config.sources.stdin {
        let source = StdinSource::new(Arc::clone(&shutdown));
        runner.spawn(source, handle.clone());
        eprintln!("Source: stdin");
    }

    for path in &config.sources.files {
        let source = FileTailer::new(path.clone(), Arc::clone(&shutdown));
        runner.spawn(source, handle.clone());
        eprintln!("Source: file {}", path.display());
    }

    for addr in &config.sources.tcp {
        let source = TcpSource::bind(addr.as_str(), Arc::clone(&shutdown))
            .with_context(|| format!("failed to bind TCP {addr}"))?;
        runner.spawn(source, handle.clone());
        eprintln!("Source: tcp {addr}");
    }

    for addr in &config.sources.udp {
        let source = UdpSource::bind(addr.as_str(), Arc::clone(&shutdown))
            .with_context(|| format!("failed to bind UDP {addr}"))?;
        runner.spawn(source, handle.clone());
        eprintln!("Source: udp {addr}");
    }

    eprintln!(
        "Config: memtable_threshold={}, compaction_threshold={}, compaction={}, flush_mode={:?}",
        config.storage.memtable_flush_threshold,
        config.storage.compaction_threshold,
        config.storage.compaction_enabled,
        config.wal.flush_mode
    );
    eprintln!("Ready. Press Ctrl+C to shutdown.");

    let ingester_thread = thread::spawn(move || ingester.run());

    let source_results = runner.join_all();

    let received_sigint = sigint_flag.load(Ordering::Relaxed);
    let received_sigterm = sigterm_flag.load(Ordering::Relaxed);
    handle.shutdown();

    let ingester_result = ingester_thread.join().expect("ingester thread panicked");

    let mut exit_code = ExitCode::SUCCESS;

    for result in source_results {
        if let Err(ref e) = result {
            // Channel disconnects are expected during signal shutdown because the ingester closes
            // its receiver before sources finish draining.
            if matches!(e, source::Error::ChannelDisconnected)
                && (received_sigint || received_sigterm)
            {
                continue;
            }
            eprintln!("source error: {e}");
            exit_code = ExitCode::FAILURE;
        }
    }

    let stats = ingester_result;
    eprintln!("Shutdown complete.");
    eprintln!("  Entries ingested: {}", stats.entries_ingested);
    eprintln!("  Bytes ingested: {}", stats.bytes_ingested);
    if stats.entries_failed > 0 {
        eprintln!("  Entries failed: {}", stats.entries_failed);
    }

    // Signal-based exit codes take precedence over source errors to accurately report termination
    // cause to process supervisors and container orchestrators.
    if received_sigint {
        return Ok(130.into());
    }

    if received_sigterm {
        return Ok(143.into());
    }

    Ok(exit_code)
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert();
}
