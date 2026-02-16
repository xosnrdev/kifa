<p align="center">
  <h1 align="center">Kifa (KEE-fah)</h1>
  <p align="center"><b>Crash-proof local logging for POS and mobile money systems</b></p>
</p>

<p align="center">
  <a href="https://github.com/xosnrdev/kifa/actions/workflows/ci.yml"><img src="https://github.com/xosnrdev/kifa/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/xosnrdev/kifa/releases/latest"><img src="https://img.shields.io/github/v/release/xosnrdev/kifa?include_prereleases&label=release" alt="Release"></a>
  <a href="LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue" alt="License"></a>
  <a href="https://xosnrdev.github.io/kifa/"><img src="https://img.shields.io/badge/docs-xosnrdev.github.io%2Fkifa-c8a46e" alt="Docs"></a>
</p>

## Table of Contents

- [What is Kifa?](#what-is-kifa)
- [Quick Start](#quick-start)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## What is Kifa?

Kifa is a storage engine for transaction logs on edge devices. It uses a write-ahead log with configurable fsync to guarantee that **every confirmed write survives power failure**.

Built for POS terminals and mobile money agents operating where power is unreliable, connectivity is intermittent, and every transaction matters.

| Constraint         | How Kifa Addresses It                                     |
| ------------------ | --------------------------------------------------------- |
| Unreliable power   | Configurable fsync: batched, per-write, or emergency mode |
| Limited resources  | Single static binary, minimal memory footprint            |
| Offline operation  | No network dependencies, local storage only               |
| Audit requirements | Immutable append-only log with timestamp ordering         |

## Quick Start

Install from source (requires Rust nightly 1.93.0+):

```bash
cargo install --git https://github.com/xosnrdev/kifa.git
```

Or download a [pre-built binary](https://github.com/xosnrdev/kifa/releases/latest) for Linux (x86_64, aarch64, armhf).

Ingest a transaction and query it back:

```bash
echo '{"txn":"TXN-001","amount":5000,"status":"completed"}' | \
  kifa daemon --stdin -d ./data

kifa query -d ./data
```

See the [Quick Start guide](https://xosnrdev.github.io/kifa/quick-start/) for a full walkthrough with crash recovery verification.

## Documentation

- [xosnrdev.github.io/kifa](https://xosnrdev.github.io/kifa/)

## Contributing

Want to contribute? Please read:

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)

## License

Dual-licensed under your choice of:

- [MIT License](LICENSE-MIT)
- [Apache License 2.0](LICENSE-APACHE)
