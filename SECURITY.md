# Security Policy

Kifa is a crash-proof logging system designed for POS terminals and mobile money
systems. Data integrity and durability are core to its purpose. Security reports
are taken seriously.

## Supported versions

| Version            | Supported |
| ------------------ | --------- |
| Latest on `master` | Yes       |
| Older releases     | No        |

This table will be updated as the project matures and tagged releases are
introduced.

## Reporting a vulnerability

**Do NOT open a public GitHub issue for security vulnerabilities.**

Report through one of these channels, in order of preference:

1. **GitHub Private Vulnerability Reporting** — Use the "Report a vulnerability"
   button under the
   [Security tab](https://github.com/xosnrdev/kifa/security/advisories/new).
2. **Email** — Send a report to **xosnrdev@gmail.com** with the subject line
   "Kifa Security Report". Include a description of the vulnerability,
   reproduction steps, affected versions, and your assessment of the impact.

## What to expect

- Acknowledgment of receipt within 7 days.
- Assessment and initial response within 30 days.
- Target resolution within 90 days of the initial report.
- Credit in the security advisory unless you request otherwise.

Kifa is solo-maintained. These timelines are targets, not guarantees. Complex
issues may take longer.

## Scope

**In scope:**

- The `kifa` binary crate and `lib-kifa` library crate.
- Storage engine integrity (WAL, SSTables, manifest, compaction).
- Data corruption or loss scenarios.
- Unauthorized data access through the TCP/UDP listeners.
- Denial of service through crafted input.

**Out of scope:**

- Third-party dependencies. Report these upstream, but notify Kifa so the
  dependency can be updated.
- Theoretical attacks requiring physical access to the host machine.
- Vulnerabilities in development tooling (CI, linters).

## Security considerations for deployers

- Kifa does not encrypt data at rest. Deployers handling sensitive transaction
  logs should apply filesystem-level encryption.
- Restrict file permissions on the Kifa data directory to the service user.
- TCP/UDP listeners bind to the address specified at startup. Bind to
  `127.0.0.1` unless remote ingestion is explicitly needed.
- Review flush mode selection (`normal`, `cautious`, `emergency`) for the
  deployment's durability requirements.

## Disclosure policy

Kifa follows coordinated disclosure. Vulnerabilities are disclosed publicly only
after a fix is available or 90 days have elapsed, whichever comes first.
Security advisories are published through
[GitHub Security Advisories](https://github.com/xosnrdev/kifa/security/advisories).
