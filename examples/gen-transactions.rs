//! Synthetic POS and mobile money transaction log generator.
//!
//! Outputs one JSON object per line (JSONL) to stdout. Each line represents a single POS or mobile
//! money transaction with realistic fields, amounts, and weighted distributions. Kifa reads these
//! over stdin or TCP/UDP for demos and crash testing.

use std::io::{self, BufWriter, Write};
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

use clap::Parser;
use fastrand::Rng;
use jiff::Timestamp;
use jiff::tz::TimeZone;

fn main() -> ExitCode {
    // Broken pipe produces ErrorKind::BrokenPipe when the downstream reader (e.g. Kifa) is killed.
    // The generator exits cleanly instead of panicking so the crash-test harness works correctly.
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gen-transactions: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    let args = Args::parse();

    let mut rng = if args.seed != 0 { Rng::with_seed(args.seed) } else { Rng::new() };

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    let sleep_dur = 1_000_000_000u64.checked_div(args.rate).map(Duration::from_nanos);

    let infinite = args.count == 0;
    let mut seq = 0;

    loop {
        seq += 1;

        if !infinite && seq > args.count {
            break;
        }

        let (timestamp, date) = now_timestamps();

        write_transaction(
            &mut writer,
            &mut rng,
            seq,
            &timestamp,
            &date,
            args.terminals,
            args.agents,
        )?;
        writer.flush()?;

        if let Some(dur) = sleep_dur {
            thread::sleep(dur);
        }
    }

    Ok(())
}

#[derive(Parser)]
#[command(about = "Generate synthetic POS and mobile money transaction logs")]
struct Args {
    /// Number of transactions to generate (0 = infinite).
    #[arg(short = 'n', long, default_value_t = 1000)]
    count: u64,

    /// Transactions per second (0 = no throttle).
    #[arg(short, long, default_value_t = 100)]
    rate: u64,

    /// Number of unique terminal IDs.
    #[arg(short, long, default_value_t = 10)]
    terminals: u16,

    /// Number of unique agent IDs.
    #[arg(short, long, default_value_t = 5)]
    agents: u16,

    /// RNG seed for reproducible output (0 = use system time).
    #[arg(short, long, default_value_t = 0)]
    seed: u64,
}

/// Selects an index from a slice of weights. Each entry represents a relative probability, so
/// [60, 20, 15, 5] picks index 0 sixty percent of the time.
fn weighted_index(rng: &mut Rng, weights: &[u32]) -> usize {
    let total: u32 = weights.iter().sum();
    let mut threshold = rng.u32(0..total);
    for (i, &w) in weights.iter().enumerate() {
        if threshold < w {
            return i;
        }
        threshold -= w;
    }
    weights.len() - 1
}

/// Formats the current UTC time as an RFC 3339 string with millisecond precision and a YYYYMMDD
/// date stamp for receipt IDs.
fn now_timestamps() -> (String, String) {
    let ts = Timestamp::now();
    let rfc3339 = format!("{ts:.3}");

    let zdt = ts.to_zoned(TimeZone::UTC);
    let date_stamp = format!("{:04}{:02}{:02}", zdt.year(), zdt.month(), zdt.day());

    (rfc3339, date_stamp)
}

const TXN_TYPES: &[&str] = &["payment", "withdrawal", "deposit", "transfer"];
const TXN_TYPE_WEIGHTS: &[u32] = &[60, 20, 15, 5];

const METHODS: &[&str] = &["mobile_money", "cash", "card"];
const METHOD_WEIGHTS: &[u32] = &[70, 20, 10];

const STATUSES: &[&str] = &["completed", "failed", "pending"];
const STATUS_WEIGHTS: &[u32] = &[95, 3, 2];

fn gen_amount(rng: &mut Rng, txn_type: &str) -> u64 {
    match txn_type {
        "withdrawal" => rng.u64(100..=150_000),
        "deposit" => rng.u64(100..=500_000),
        "transfer" => rng.u64(50..=100_000),
        _ => rng.u64(50..=50_000),
    }
}

fn gen_phone(rng: &mut Rng) -> String {
    let digits = rng.u64(10_000_000..=99_999_999);
    format!("2547{digits}")
}

/// Writes a single transaction as a JSON line to the provided writer.
fn write_transaction(
    w: &mut impl Write,
    rng: &mut Rng,
    seq: u64,
    timestamp: &str,
    date: &str,
    terminal_count: u16,
    agent_count: u16,
) -> io::Result<()> {
    let terminal_id = rng.u16(1..=terminal_count);
    let agent_id = rng.u16(1..=agent_count);

    let txn_type = TXN_TYPES[weighted_index(rng, TXN_TYPE_WEIGHTS)];
    let amount = gen_amount(rng, txn_type);
    let method = METHODS[weighted_index(rng, METHOD_WEIGHTS)];
    let sender = gen_phone(rng);
    let receiver = gen_phone(rng);
    let status = STATUSES[weighted_index(rng, STATUS_WEIGHTS)];

    writeln!(
        w,
        r#"{{"txn_id":"TXN-{seq:08}","terminal_id":"T-{terminal_id:04}","agent_id":"A-{agent_id:03}","timestamp":"{timestamp}","type":"{txn_type}","amount":{amount},"currency":"KES","method":"{method}","sender":"{sender}","receiver":"{receiver}","status":"{status}","receipt":"R-{date}-{seq:08}"}}"#
    )
}
