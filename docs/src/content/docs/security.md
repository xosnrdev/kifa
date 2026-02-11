---
title: Security Policy
description: Reporting vulnerabilities, supported versions, and security considerations for deployers.
---

Kifa is a crash-proof logging system designed for POS terminals and mobile money systems. Data integrity and durability are core to its purpose. Security reports are taken seriously.

## Supported Versions

| Version            | Supported |
| ------------------ | --------- |
| Latest on `master` | Yes       |
| Older releases     | No        |

## Reporting a Vulnerability

**Do NOT open a public GitHub issue for security vulnerabilities.**

Report through one of these channels, in order of preference:

1. **GitHub Private Vulnerability Reporting**: Use the "Report a vulnerability" button under the [Security tab](https://github.com/xosnrdev/kifa/security/advisories/new).
2. **Email**: Send a report to **xosnrdev@gmail.com** with the subject line "Kifa Security Report". Include a description, reproduction steps, affected versions, and impact assessment.

## What to Expect

| Stage                           | Target                                                 |
| ------------------------------- | ------------------------------------------------------ |
| Acknowledgment                  | Within 7 days                                          |
| Assessment and initial response | Within 30 days                                         |
| Resolution                      | Within 90 days                                         |
| Credit                          | In the security advisory, unless you request otherwise |

Kifa is solo-maintained. These timelines are targets, not guarantees.

## Scope

**In scope:**

- The `kifa` binary crate and `lib-kifa` library crate
- Storage engine integrity (WAL, SSTables, manifest, compaction)
- Data corruption or loss scenarios
- Unauthorized data access through TCP/UDP listeners
- Denial of service through crafted input

**Out of scope:**

- Third-party dependencies (report upstream, then notify Kifa)
- Theoretical attacks requiring physical access to the host
- Vulnerabilities in development tooling

## Security Considerations for Deployers

- Kifa does not encrypt data at rest. Apply filesystem-level encryption for sensitive transaction logs.
- Restrict file permissions on the data directory to the service user.
- TCP/UDP listeners bind to the address specified at startup. Bind to `127.0.0.1` unless remote ingestion is explicitly needed.
- Review [flush mode selection](/kifa/guides/flush-modes/) for the deployment's durability requirements.

## Disclosure Policy

Kifa follows coordinated disclosure. Vulnerabilities are disclosed publicly only after a fix is available or 90 days have elapsed, whichever comes first. Advisories are published through [GitHub Security Advisories](https://github.com/xosnrdev/kifa/security/advisories).
