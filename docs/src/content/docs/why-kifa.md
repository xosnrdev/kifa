---
title: Why Kifa
description: What Kifa is, what problem it solves, and how it compares to alternatives.
---

Kifa is a storage engine for transaction logs on edge devices. It uses a write-ahead log with configurable fsync to guarantee that **every confirmed write survives power failure**. Built for POS terminals and mobile money agents operating in unreliable environments.

When a payment terminal loses power mid-transaction, Kifa keeps the data written before the crash. No SQLite. No external database. A single binary that does one thing well.

## Built for the Edge

| Constraint         | How Kifa Addresses It                                          |
| ------------------ | -------------------------------------------------------------- |
| Unreliable power   | Configurable fsync modes, from batched to per-write durability |
| Limited resources  | Single static binary, minimal memory footprint                 |
| Offline operation  | No network dependencies, everything stored locally             |
| Audit requirements | Immutable append-only log with timestamp ordering              |

## What Makes It Different

| Feature                 | Kifa | SQLite  | Plain Files |
| ----------------------- | :--: | :-----: | :---------: |
| Crash-proof writes      | Yes  |   Yes   |     No      |
| No runtime dependencies | Yes  |   No    |     Yes     |
| Configurable durability | Yes  | Limited |     No      |
| Append-optimized        | Yes  |   No    |     Yes     |
| Query by time range     | Yes  |   Yes   |     No      |
| Built-in compaction     | Yes  |   Yes   |     No      |
| Direct I/O (Linux)      | Yes  |   No    |     No      |

## What Kifa Is Not

Kifa is not a general-purpose database. It does not support random key-value access, SQL queries, or multi-table schemas. It is not a message queue, and it is not a replacement for a full observability stack.

Kifa is a durable local log for edge devices. Entries go in, entries come out, and nothing is lost in between.
