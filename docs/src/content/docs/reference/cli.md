---
title: CLI Reference
description: Complete reference for all Kifa commands, flags, and options.
---

## Global Flags

| Flag                    | Description                                          |
| ----------------------- | ---------------------------------------------------- |
| `-d, --data-dir <PATH>` | Directory for WAL, SSTables, and metadata (required) |
| `-c, --config <PATH>`   | Path to TOML configuration file                      |
| `-h, --help`            | Print help                                           |
| `-V, --version`         | Print version                                        |

## `daemon`

Run as log ingestion daemon. This is the default command when no subcommand is specified.

### Source Flags

| Flag            | Description                                               |
| --------------- | --------------------------------------------------------- |
| `--stdin`       | Read log lines from standard input                        |
| `--file <PATH>` | Tail a log file (repeatable)                              |
| `--tcp <ADDR>`  | Listen on TCP address, e.g. `127.0.0.1:5514` (repeatable) |
| `--udp <ADDR>`  | Listen on UDP address, e.g. `0.0.0.0:5515` (repeatable)   |

At least one source is required.

### Storage Flags

| Flag                           | Description                                       | Default  |
| ------------------------------ | ------------------------------------------------- | -------- |
| `--flush-mode <MODE>`          | WAL flush mode: `normal`, `cautious`, `emergency` | `normal` |
| `--segment-size-mib <N>`       | WAL segment size in MiB (minimum: 1)              | `16`     |
| `--memtable-threshold-mib <N>` | Flush memtable after N MiB (minimum: 1)           | `4`      |
| `--compaction-threshold <N>`   | Compact after N SSTables (minimum: 2)             | `4`      |
| `--no-compaction`              | Disable background compaction                     |          |
| `--channel-capacity <N>`       | Internal channel buffer size (minimum: 1)         | `1024`   |

`--compaction-threshold` and `--no-compaction` are mutually exclusive.

## `query`

Query stored log entries and print to stdout.

| Flag                 | Description                                 | Default |
| -------------------- | ------------------------------------------- | ------- |
| `--from-lsn <N>`     | Start from LSN (inclusive)                  |         |
| `--to-lsn <N>`       | End at LSN (inclusive)                      |         |
| `--from-time <TIME>` | Filter from time                            |         |
| `--to-time <TIME>`   | Filter until time                           |         |
| `-f, --format <FMT>` | Output format: `text`, `json`, `csv`, `hex` | `text`  |

### Time Format

Relative: `1h`, `30m`, `2d`, `1h30m`

Absolute: `YYYY-MM-DD`, `YYYY-MM-DD HH:mm:ss`, ISO 8601

## `export`

Export log entries to a file. Writes are atomic.

| Flag                  | Description                                 | Default  |
| --------------------- | ------------------------------------------- | -------- |
| `-o, --output <PATH>` | Output file path                            | Required |
| `--from-lsn <N>`      | Start from LSN (inclusive)                  |          |
| `--to-lsn <N>`        | End at LSN (inclusive)                      |          |
| `--from-time <TIME>`  | Filter from time                            |          |
| `--to-time <TIME>`    | Filter until time                           |          |
| `-f, --format <FMT>`  | Output format: `text`, `json`, `csv`, `hex` | `json`   |

## `stats`

Print storage statistics to stdout. Takes no additional flags beyond the global ones.

Output includes:

- Total entry count
- SSTable count and checkpoint LSN
- Memtable entry count
- Current flush mode
- Time range of stored entries
- LSN gap detection
