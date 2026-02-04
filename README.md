<p align="center">
  <h1 align="center">Kifa (KEE-fah)</h1>
  <p align="center"><b>Crash-proof local logging for POS and mobile money systems</b></p>
</p>

<p align="center">
  <a href="#what-is-kifa">What is Kifa?</a> •
  <a href="#why-kifa">Why Kifa?</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#installation">Installation</a> •
  <a href="#usage">Usage</a> •
  <a href="#security-considerations">Security Considerations</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#crash-testing">Crash Testing</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#deployment">Deployment</a> •
  <a href="#contributing">Contributing</a>
</p>

## What is Kifa?

Kifa is a storage engine for transaction logs on edge devices. It uses a write-ahead log with configurable fsync to guarantee that **every confirmed write survives power failure**. Built for POS terminals and mobile money agents operating in unreliable environments.

When your payment terminal loses power mid-transaction, Kifa keeps the data written before the crash. No SQLite. No external database. Just a single binary that does one thing well.

## Why Kifa?

### Built for the Edge

| Constraint         | How Kifa Addresses It                                          |
| ------------------ | -------------------------------------------------------------- |
| Unreliable power   | Configurable fsync modes, from batched to per-write durability |
| Limited resources  | Single static binary, minimal memory footprint                 |
| Offline operation  | No network dependencies, everything stored locally             |
| Audit requirements | Immutable append-only log with LSN ordering                    |

### What Makes It Different

| Feature                 | Kifa | SQLite  | Plain Files |
| ----------------------- | :--: | :-----: | :---------: |
| Crash-proof writes      |  ✓   |    ✓    |      ✗      |
| No runtime dependencies |  ✓   |    ✗    |      ✓      |
| Configurable durability |  ✓   | Limited |      ✗      |
| Append-optimized        |  ✓   |    ✗    |      ✓      |
| Query by time range     |  ✓   |    ✓    |      ✗      |
| Built-in compaction     |  ✓   |    ✓    |      ✗      |
| Direct I/O (Linux)      |  ✓   |    ✗    |      ✗      |

## Quick Start

```bash
# Clone and build
git clone https://github.com/xosnrdev/kifa.git
cd kifa
cargo build --release

# Start ingesting from stdin
echo '{"txn":"TXN-001","amount":5000,"status":"completed"}' | \
  ./target/release/kifa daemon --stdin -d ./data

# Query stored entries
./target/release/kifa query -d ./data

# View storage statistics
./target/release/kifa stats -d ./data
```

**Output from `stats`:**

```
Storage:
  Total entries: 1
  SSTables: 0 (checkpoint LSN: 0)
  Memtable: 1 entries

WAL:
  Flush mode: Normal

Health:
  Time range: 2026-01-31 18:30:00 UTC - 2026-01-31 18:30:00 UTC
  Gaps: none
```

## Installation

### Requirements

- **Rust 1.93.0+** (nightly toolchain)
- **Supported platforms**: Linux (recommended), macOS, Windows

### Building from Source

```bash
git clone https://github.com/xosnrdev/kifa.git
cd kifa
cargo build --release
```

Binary location: `./target/release/kifa`

### Building with Examples

Also included: a transaction generator and crash-test harness:

```bash
cargo build --release --examples
```

This builds:

- `./target/release/examples/gen-transactions`: Synthetic POS transaction generator
- `./target/release/examples/crash-test`: Crash recovery verification harness

For true durability validation, run crash tests with LazyFS in Docker (see [Crash Testing](#crash-testing) below).

## Usage

### Commands Overview

| Command  | Description                 |
| -------- | --------------------------- |
| `daemon` | Run as log ingestion daemon |
| `query`  | Query stored log entries    |
| `export` | Export logs to file         |
| `stats`  | Show storage statistics     |

### Running as Daemon

Daemon mode reads log lines from one or more sources and persists them to disk.

#### Input Sources

| Flag      | Description                  | Example                                       |
| --------- | ---------------------------- | --------------------------------------------- |
| `--stdin` | Read from standard input     | `echo "log" \| kifa daemon --stdin -d ./data` |
| `--file`  | Tail a log file (repeatable) | `--file /var/log/app.log`                     |
| `--tcp`   | Listen on TCP address        | `--tcp 127.0.0.1:5514`                        |
| `--udp`   | Listen on UDP address        | `--udp 0.0.0.0:5515`                          |

#### Examples

**Pipe from another process:**

```bash
my-pos-app | kifa daemon --stdin -d /var/lib/kifa
```

**Tail multiple log files:**

```bash
kifa daemon \
  --file /var/log/transactions.log \
  --file /var/log/payments.log \
  -d /var/lib/kifa
```

**Network ingestion:**

```bash
kifa daemon --tcp 127.0.0.1:5514 --udp 0.0.0.0:5515 -d /var/lib/kifa
```

**Combined sources:**

```bash
kifa daemon \
  --stdin \
  --file /var/log/app.log \
  --tcp 127.0.0.1:5514 \
  -d /var/lib/kifa
```

### Querying Logs

Retrieve entries by LSN or time range.

#### Query Options

| Flag          | Description                | Example                                                 |
| ------------- | -------------------------- | ------------------------------------------------------- |
| `--from-lsn`  | Start from LSN (inclusive) | `--from-lsn 100`                                        |
| `--to-lsn`    | End at LSN (inclusive)     | `--to-lsn 500`                                          |
| `--from-time` | Filter from time           | `--from-time 1h` or `--from-time "2026-01-29 12:00:00"` |
| `--to-time`   | Filter until time          | `--to-time "2026-01-30"`                                |
| `--format`    | Output format              | `text`, `json`, `csv`, `hex`                            |

#### Time Format Examples

```bash
# Relative time (from now)
kifa query -d ./data --from-time 1h          # Last hour
kifa query -d ./data --from-time 30m         # Last 30 minutes
kifa query -d ./data --from-time 2d          # Last 2 days
kifa query -d ./data --from-time 1h30m       # Last 1.5 hours

# Absolute time
kifa query -d ./data --from-time "2026-01-29 12:00:00"
kifa query -d ./data --from-time "2026-01-29T12:00:00Z"
kifa query -d ./data --from-time "2026-01-29"  # Midnight
```

#### Output Formats

```bash
# Plain text (default)
kifa query -d ./data --format text
# [1] 2026-01-31 18:30:00 UTC {"txn":"TXN-001","amount":5000}

# JSON Lines
kifa query -d ./data --format json
# {"lsn":1,"timestamp":"2026-01-31 18:30:00 UTC","timestamp_ms":1738348200000,"data":"{...}"}

# CSV
kifa query -d ./data --format csv
# lsn,timestamp,timestamp_ms,data
# 1,"2026-01-31 18:30:00 UTC",1738348200000,"{...}"

# Hex dump
kifa query -d ./data --format hex
# 0000000000000001 0000019c099fe600 7b22...
```

### Exporting Data

Export logs to a file with atomic writes (partial exports are never visible).

```bash
# Export last 24 hours as JSON
kifa export -d ./data --from-time 24h --format json -o transactions.json

# Export specific LSN range as CSV
kifa export -d ./data --from-lsn 1 --to-lsn 1000 --format csv -o audit.csv
```

## Configuration

Kifa uses a three-layer configuration system with the following precedence:

**CLI flags > Environment variables > TOML file**

### Configuration File

Place `kifa.toml` in your data directory or specify with `-c /path/to/config.toml`.

```toml
[storage]
# Flush memtable to SSTable after this many MiB (minimum: 1)
memtable_flush_threshold_mib = 4

# Compact after this many SSTables (minimum: 2)
compaction_threshold = 4

# Enable background compaction
compaction_enabled = true

[wal]
# Sync strategy: "normal", "cautious", or "emergency"
flush_mode = "normal"

# WAL segment size in MiB (minimum: 1, must be 4 KiB-aligned)
segment_size_mib = 16

[ingester]
# Internal channel buffer size (minimum: 1)
channel_capacity = 1024

[sources]
# Enable stdin ingestion
stdin = false

# Files to tail
files = ["~/logs/transactions.log"]

# TCP listen addresses
tcp = ["127.0.0.1:5514"]

# UDP listen addresses
udp = ["0.0.0.0:5515"]
```

### Environment Variables

All settings can be overridden via environment variables with the `KIFA_` prefix:

| Variable                            | Description                       | Default  |
| ----------------------------------- | --------------------------------- | -------- |
| `KIFA_DATA_DIR`                     | Storage directory                 | Required |
| `KIFA_FLUSH_MODE`                   | `normal`, `cautious`, `emergency` | `normal` |
| `KIFA_SEGMENT_SIZE_MIB`             | WAL segment size                  | `16`     |
| `KIFA_MEMTABLE_FLUSH_THRESHOLD_MIB` | Memtable flush threshold          | `4`      |
| `KIFA_COMPACTION_THRESHOLD`         | SSTable compaction trigger        | `4`      |
| `KIFA_COMPACTION_ENABLED`           | Enable compaction                 | `true`   |
| `KIFA_CHANNEL_CAPACITY`             | Ingester buffer size              | `1024`   |
| `KIFA_STDIN`                        | Read from stdin                   | `false`  |
| `KIFA_FILES`                        | Comma-separated file paths        |          |
| `KIFA_TCP`                          | Comma-separated TCP addresses     |          |
| `KIFA_UDP`                          | Comma-separated UDP addresses     |          |
| `KIFA_LOG`                          | Log level filter for env_logger   |          |

### CLI Flags

```bash
kifa daemon --help
```

| Flag                       | Description                               |
| -------------------------- | ----------------------------------------- |
| `-d, --data-dir`           | Directory for WAL, SSTables, and metadata |
| `-c, --config`             | Path to TOML configuration file           |
| `--flush-mode`             | WAL flush mode                            |
| `--segment-size-mib`       | WAL segment size in MiB                   |
| `--memtable-threshold-mib` | Flush memtable after this many MiB        |
| `--compaction-threshold`   | Compact after this many SSTables          |
| `--no-compaction`          | Disable background compaction             |
| `--channel-capacity`       | Internal channel buffer size              |

## Flush Modes

Flush modes control when data is fsync'd to disk. Choose based on your durability requirements and power stability.

| Mode        | Behavior                           | Data at Risk              | Use When                           |
| ----------- | ---------------------------------- | ------------------------- | ---------------------------------- |
| `normal`    | Batch sync every ~50 writes        | Up to 49 entries          | Stable power, maximum throughput   |
| `cautious`  | Sync after each write              | None (after call returns) | Elevated risk, possible brown-outs |
| `emergency` | Sync immediately, pause compaction | None                      | Power failure imminent             |

### Setting Flush Mode

**At startup:**

```bash
kifa daemon --stdin --flush-mode cautious -d ./data
```

**Via environment:**

```bash
KIFA_FLUSH_MODE=emergency kifa daemon --stdin -d ./data
```

**At runtime (Unix only):**

```bash
# Escalate to next level (Normal → Cautious → Emergency)
kill -USR1 $(pgrep kifa)
```

### Performance Considerations

| Mode        | Typical Throughput  | Notes                                |
| ----------- | ------------------- | ------------------------------------ |
| `normal`    | 10,000+ entries/sec | Linux Direct I/O bypasses page cache |
| `cautious`  | 100-300 entries/sec | Depends on storage fsync speed       |
| `emergency` | Same as cautious    | Also pauses compaction               |

## Architecture

### Data Flow

```
┌─────────┐  ┌──────────┐  ┌─────────┐  ┌─────────┐
│  stdin  │  │   File   │  │   TCP   │  │   UDP   │
└────┬────┘  └────┬─────┘  └────┬────┘  └────┬────┘
     │            │             │            │
     └────────────┴──────┬──────┴────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   Ingester Channel  │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   Write-Ahead Log   │  ← Durability guarantee
              │   (16 MiB segments) │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │      Memtable       │  ← In-memory, sorted by LSN
              └──────────┬──────────┘
                         │ Flush (threshold reached)
                         ▼
              ┌─────────────────────┐
              │      SSTables       │  ← Immutable, sorted files
              └──────────┬──────────┘
                         │ Compaction (background)
                         ▼
              ┌─────────────────────┐
              │   Merged SSTables   │
              └─────────────────────┘
```

### Components

| Component      | Description                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------------ |
| **WAL**        | 16 MiB segments with dual-CRC validation. Direct I/O on Linux bypasses page cache for true durability. |
| **Memtable**   | In-memory buffer holding recent writes, sorted by LSN.                                                 |
| **SSTables**   | Immutable sorted files. Created when memtable exceeds threshold.                                       |
| **Compaction** | Background process that merges SSTables to reclaim space and improve read performance.                 |
| **Manifest**   | Tracks database state: which SSTables exist, checkpoint LSN, etc.                                      |

### Entry Format

Each log entry contains:

| Field     | Size     | Description                                    |
| --------- | -------- | ---------------------------------------------- |
| LSN       | 8 bytes  | Log sequence number (monotonically increasing) |
| Timestamp | 8 bytes  | Unix timestamp in milliseconds                 |
| Data      | Variable | Raw payload (max 1 MiB)                        |

Entries are validated with:

- Header CRC (protects length field)
- Data CRC (protects LSN + timestamp + payload)
- Magic trailer (detects incomplete writes)

## Deployment

Kifa runs as a single binary. Start it however your system starts processes.

### Linux with Systemd

A hardened systemd service file is included at `systemd/kifa.service`:

```bash
# Copy service file
sudo cp systemd/kifa.service /etc/systemd/system/

# Edit configuration
sudo systemctl edit kifa
# Add: Environment=KIFA_TCP=127.0.0.1:5514

# Enable and start
sudo systemctl enable --now kifa

# Check status
sudo systemctl status kifa
journalctl -u kifa -f
```

Security hardening included:

- `DynamicUser=yes`: Runs as ephemeral user
- `ProtectSystem=strict`: Read-only filesystem except data dir
- `PrivateNetwork=no`: Allows TCP/UDP listeners
- `MemoryDenyWriteExecute=yes`: Prevents code injection

### Signal Handling

| Signal            | Action                                              |
| ----------------- | --------------------------------------------------- |
| `SIGINT` (Ctrl+C) | Graceful shutdown, flush pending data               |
| `SIGTERM`         | Same as SIGINT (for systemd)                        |
| `SIGUSR1`         | Escalate flush mode (Normal → Cautious → Emergency) |

Exit codes follow Unix convention:

- `0`: Clean exit
- `130`: Terminated by SIGINT
- `143`: Terminated by SIGTERM

## Security Considerations

- **No encryption at rest.** Apply filesystem-level encryption (LUKS, dm-crypt) for sensitive data.
- **Restrict data directory permissions.** Only the service user should have access.
- **Bind listeners carefully.** Use `127.0.0.1` unless remote ingestion is explicitly needed.
- **Review flush mode.** Balance durability requirements against performance.

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Contributing

Want to contribute? Please read:

- [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community standards
- [SECURITY.md](SECURITY.md) for responsible disclosure

### Quick Development Commands

```bash
# Format check
cargo fc

# Lint
cargo cw

# Run tests
cargo tw

# Run doc tests
cargo twdoc

# Run crash test (SIGKILL-based, see note below)
cargo ct
```

### Crash Testing

`cargo ct` uses SIGKILL to simulate crashes. On Linux, this can produce false positives because the kernel may persist buffered writes after the process dies. For true durability validation, use the Docker-based LazyFS test:

```bash
# Build the crash-test image
docker build -f docker/Dockerfile.crash-test -t kifa-crash-test .

# Run with cautious mode (zero data loss expected)
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode cautious

# Run with normal mode (gaps allowed, up to 49 entries at risk)
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode normal
```

LazyFS intercepts filesystem calls and discards unsynced data when clearing its cache, simulating actual power loss behavior.

## License

Dual-licensed under your choice of:

- [MIT License](LICENSE-MIT)
- [Apache License 2.0](LICENSE-APACHE)
