# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-02-20

### Added

- Append-only LSM-tree storage engine with write-ahead log durability.
- Three flush modes: `cautious` (per-write fsync, default), `normal` (batched fsync, ~7x throughput), `emergency` (fsync + pause compaction).
- Four input sources: stdin piping, file tailing, TCP listener, UDP listener. Sources can be combined.
- `query` and `export` commands with `--from`/`--to` time range filters. Accepts relative offsets (`1h`, `30m`, `2d`) and absolute timestamps. `--limit` caps results (default: 1000; use `--limit 0` for all).
- Export writes atomically via temp-file-and-rename; no partial output on interruption.
- Four output formats: `text` (UTC timestamp + data, embedded newlines escaped), `json`, `csv`, `hex`.
- `stats` command for storage health and entry counts.
- Background compaction merging SSTables when threshold is reached (default: 4).
- Crash recovery with dual CRC validation (header + data) and magic trailer detection.
- Direct I/O (`O_DIRECT`) on Linux, bypassing the kernel page cache for true storage-level durability.
- Runtime flush mode escalation via `SIGUSR1` (Cautious to Emergency).
- Three-layer configuration: TOML file, environment variables (`KIFA_` prefix), CLI flags. Each layer overrides the previous.
- Systemd service file with security hardening (`DynamicUser`, `ProtectSystem=strict`, `MemoryDenyWriteExecute`).
- Linux packages: `.deb`, `.rpm`, and static musl binaries for x86_64, aarch64, and armhf.
- Graceful shutdown on `SIGINT`/`SIGTERM` with pending data flush.
- `lib-kifa` crate for embedding the storage engine directly in Rust applications.
- Web documentation site at [xosnrdev.github.io/kifa](https://xosnrdev.github.io/kifa/).

[Unreleased]: https://github.com/xosnrdev/kifa/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/xosnrdev/kifa/releases/tag/v1.0.0
