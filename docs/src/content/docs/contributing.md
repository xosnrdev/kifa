---
title: Contributing
description: Development setup, code quality standards, and the pull request process.
---

Contributions are welcome. Kifa is solo-maintained, so response times on issues and pull requests vary. All participants are expected to follow the [Code of Conduct](https://github.com/xosnrdev/kifa/blob/master/CODE_OF_CONDUCT.md).

## Ways to Contribute

- **Report bugs**: Open an issue using the [bug report](https://github.com/xosnrdev/kifa/issues/new?template=01-bug-report.yml) template.
- **Suggest features**: Open an issue using the [feature request](https://github.com/xosnrdev/kifa/issues/new?template=02-feature-request.yml) template.
- **Submit code**: Fork the repository, create a branch, and open a pull request.

## Development Setup

Kifa requires the Rust nightly toolchain. Minimum supported version: 1.93.0.

```bash
git clone https://github.com/xosnrdev/kifa.git
cd kifa
cargo build --workspace
cargo tw
```

## CI Commands

The project uses aliased commands in `.cargo/config.toml`:

| Command       | Description                |
| ------------- | -------------------------- |
| `cargo fc`    | Format check               |
| `cargo cw`    | Clippy linting             |
| `cargo tw`    | Unit and integration tests |
| `cargo twdoc` | Documentation tests        |
| `cargo bp`    | Throughput benchmarks      |
| `cargo ct`    | Crash test (SIGKILL-based) |

Run all checks locally before pushing:

```bash
cargo fc && cargo cw && cargo tw && cargo twdoc
```

## Crash Testing

`cargo ct` uses SIGKILL to simulate crashes. On Linux, this can produce false positives because the kernel may persist buffered writes after process death.

For true durability validation, use Docker with LazyFS:

```bash
# Build the Docker image
docker build -f lazyfs/Dockerfile.crash-test -t kifa-crash-test .

# Test cautious mode (expects zero gaps)
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode cautious

# Test normal mode (allows gaps, up to 49 entries at risk)
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode normal

# Test emergency mode (expects zero gaps)
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode emergency
```

LazyFS intercepts filesystem calls and discards unsynced data when clearing its cache, simulating actual power loss behavior. The `--cap-add SYS_ADMIN --device /dev/fuse` flags are required for FUSE filesystem support.

## Code Quality Standards

Every pull request must pass:

1. `cargo fc` (formatting)
2. `cargo cw` (linting)
3. `cargo tw` and `cargo twdoc` (tests)

Comment conventions (when comments are necessary):

- Full sentences in third person singular present indicative form.
- Explain _why_, not _what_.
- Capitalized at the beginning, ended with a period.

## Pull Request Process

1. Fork the repository and create a branch from `master`.
2. Make focused, single-purpose commits.
3. Run all CI checks locally before pushing.
4. Open a pull request using the existing PR template.
5. One approval from the maintainer is required to merge.

## Security Issues

Do not open a public issue for security vulnerabilities. See the [Security Policy](/kifa/security/) for the responsible disclosure process.

## License

By submitting a pull request, contributions are licensed under the same terms as the project: MIT OR Apache-2.0, at the user's choice.
