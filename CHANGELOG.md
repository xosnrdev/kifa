# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Hoisted write buffer from `SegmentWriter` into `WalWriter`, reducing per-entry allocation overhead.
- Reduced instruction count across daemon and query paths.

## [1.0.0-rc.1] - 2026-02-06

First release candidate.

### Added

- Append-only LSM-tree storage engine with write-ahead log durability.
- Three flush modes: `normal` (batched fsync, ~7x throughput), `cautious` (per-write fsync), `emergency` (fsync + pause compaction).
- Four input sources: stdin piping, file tailing, TCP listener, UDP listener. Sources can be combined.
- Query command with LSN and time range filters. Relative time syntax (`24h`, `7d`) and absolute timestamps.
- Export command with atomic writes via temp-file-and-rename. No partial output on interruption.
- Four output formats: `text`, `json`, `csv`, `hex`.
- Stats command for storage health and entry counts.
- Background compaction merging SSTables when threshold is reached (default: 4).
- Crash recovery with dual CRC validation (header + data) and magic trailer detection.
- Direct I/O (`O_DIRECT`) on Linux, bypassing the kernel page cache for true storage-level durability.
- Runtime flush mode escalation via `SIGUSR1` (Cautious to Emergency).
- Three-layer configuration: TOML file, environment variables (`KIFA_` prefix), CLI flags. Each layer overrides the previous.
- Systemd service file with security hardening (`DynamicUser`, `ProtectSystem=strict`, `MemoryDenyWriteExecute`).
- Linux packages: `.deb`, `.rpm`, and static musl binaries for x86_64, aarch64, and armhf.
- Graceful shutdown on `SIGINT`/`SIGTERM` with pending data flush.
- `lib-kifa` crate for embedding the storage engine directly in Rust applications.

[Unreleased]: https://github.com/xosnrdev/kifa/compare/v1.0.0-rc.1...HEAD
[1.0.0-rc.1]: https://github.com/xosnrdev/kifa/releases/tag/v1.0.0-rc.1
