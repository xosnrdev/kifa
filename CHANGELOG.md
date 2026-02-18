# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0-rc.3] - 2026-02-18

### Added

- `--limit` on `query` and `export` (default: 1000). Truncation notice goes to stderr when results are capped. Use `--limit 0` to return all entries.

### Changed

- Replaced LSN with monotonic nanosecond timestamp as the primary ordering key. Entries are keyed by `timestamp_ns` at ingestion time. Existing data requires re-ingestion.
- Renamed time filter flags `--from-time`/`--to-time` to `--from`/`--to` on `query` and `export`.
- Renamed `kifa daemon` to `kifa ingest`.
- Removed the implicit default to `ingest` when no subcommand is given.
- Removed `Clone`, `Copy`, and `Default` derives from `Config` and `Stats` in `lib-kifa`.

### Fixed

- `--to` with a second-precision absolute timestamp excluded all entries within that second. The parsed value landed at the floor of the second while entries within it carried nanosecond offsets that exceeded the upper bound.

## [1.0.0-rc.2] - 2026-02-12

### Added

- Web documentation site at [xosnrdev.github.io/kifa](https://xosnrdev.github.io/kifa/).
- CI workflow for GitHub Pages deployment.
- OG image and social card meta tags (Twitter, LinkedIn, Slack, Discord).

### Changed

- Hoisted write buffer from `SegmentWriter` into `WalWriter`. The 1 MiB buffer now survives segment rotations instead of being re-zeroed on each rotation, cutting mimalloc memset from 14.6M to 1.2M instructions on a 50K-entry workload.
- Reduced instruction count across daemon and query paths: zero-alloc timestamp formatting, PAIRS lookup table for decimal output, SAFE[256] table for JSON escaping, direct `write_all` replacing `write!`/`writeln!` macros, `read_until` replacing `lines()` to skip per-line UTF-8 validation.
- Replaced `iter()` with `into_iter()` for `SstableReader`, removing unnecessary intermediate references.
- Trimmed README from 568 lines to a landing page pointing to the docs site.

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

[Unreleased]: https://github.com/xosnrdev/kifa/compare/v1.0.0-rc.3...HEAD
[1.0.0-rc.3]: https://github.com/xosnrdev/kifa/compare/v1.0.0-rc.2...v1.0.0-rc.3
[1.0.0-rc.2]: https://github.com/xosnrdev/kifa/compare/v1.0.0-rc.1...v1.0.0-rc.2
[1.0.0-rc.1]: https://github.com/xosnrdev/kifa/releases/tag/v1.0.0-rc.1
