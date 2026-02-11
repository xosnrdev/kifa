---
title: Configuration Reference
description: Complete reference for all Kifa configuration options across TOML, environment variables, and CLI flags.
---

All settings are available through three layers. Higher layers override lower layers:

**CLI flags > Environment variables > TOML file**

## Storage

| TOML Key                               | Env Variable                        | CLI Flag                   | Default | Description                                        |
| -------------------------------------- | ----------------------------------- | -------------------------- | ------- | -------------------------------------------------- |
| `storage.memtable_flush_threshold_mib` | `KIFA_MEMTABLE_FLUSH_THRESHOLD_MIB` | `--memtable-threshold-mib` | `4`     | Flush memtable to SSTable after N MiB. Minimum: 1. |
| `storage.compaction_threshold`         | `KIFA_COMPACTION_THRESHOLD`         | `--compaction-threshold`   | `4`     | Compact after N SSTables. Minimum: 2.              |
| `storage.compaction_enabled`           | `KIFA_COMPACTION_ENABLED`           | `--no-compaction`          | `true`  | Enable background compaction.                      |

## WAL

| TOML Key               | Env Variable            | CLI Flag             | Default  | Description                                                 |
| ---------------------- | ----------------------- | -------------------- | -------- | ----------------------------------------------------------- |
| `wal.flush_mode`       | `KIFA_FLUSH_MODE`       | `--flush-mode`       | `normal` | Sync strategy: `normal`, `cautious`, `emergency`.           |
| `wal.segment_size_mib` | `KIFA_SEGMENT_SIZE_MIB` | `--segment-size-mib` | `16`     | WAL segment size in MiB. Minimum: 1. Must be 4 KiB-aligned. |

## Ingester

| TOML Key                    | Env Variable            | CLI Flag             | Default | Description                               |
| --------------------------- | ----------------------- | -------------------- | ------- | ----------------------------------------- |
| `ingester.channel_capacity` | `KIFA_CHANNEL_CAPACITY` | `--channel-capacity` | `1024`  | Internal channel buffer size. Minimum: 1. |

## Sources

| TOML Key        | Env Variable | CLI Flag  | Default | Description                                                  |
| --------------- | ------------ | --------- | ------- | ------------------------------------------------------------ |
| `sources.stdin` | `KIFA_STDIN` | `--stdin` | `false` | Read from standard input.                                    |
| `sources.files` | `KIFA_FILES` | `--file`  |         | Files to tail. Env: comma-separated. CLI: repeatable.        |
| `sources.tcp`   | `KIFA_TCP`   | `--tcp`   |         | TCP listen addresses. Env: comma-separated. CLI: repeatable. |
| `sources.udp`   | `KIFA_UDP`   | `--udp`   |         | UDP listen addresses. Env: comma-separated. CLI: repeatable. |

## Other

| Env Variable    | Description                                                      |
| --------------- | ---------------------------------------------------------------- |
| `KIFA_DATA_DIR` | Storage directory. Equivalent to `-d, --data-dir`.               |
| `KIFA_LOG`      | Log level filter passed to env_logger. Example: `KIFA_LOG=info`. |
