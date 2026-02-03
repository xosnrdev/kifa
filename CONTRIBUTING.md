# Contributing to Kifa

Contributions are welcome. Kifa is solo-maintained, so response times on issues
and pull requests will vary. All participants are expected to follow the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report bugs** — Open an issue using the
  [bug report](https://github.com/xosnrdev/kifa/issues/new?template=01-bug-report.yml)
  template.
- **Suggest features** — Open an issue using the
  [feature request](https://github.com/xosnrdev/kifa/issues/new?template=02-feature-request.yml)
  template.
- **Submit code** — Fork the repository, create a branch, and open a pull
  request. See the process below.

## Development setup

Kifa requires the Rust nightly toolchain. The minimum supported version is
1.93.0, pinned in `rust-toolchain.toml`.

The project uses aliased commands configured in [.cargo/config.toml](./.cargo/config.toml) to streamline the development workflow.

```sh
git clone https://github.com/xosnrdev/kifa.git
cd kifa
cargo build --workspace
cargo tw
```

The following commands perform the full CI checks locally before pushing:

```sh
# Performs a format check.
cargo fc
# Runs clippy linting.
cargo cw
# Runs unit and integration tests.
cargo tw
# Runs documentation tests.
cargo twdoc
```

The crash test simulates POS crash scenarios. Run it with `cargo ct`.

**Note:** `cargo ct` uses SIGKILL, which can produce false positives on Linux because the kernel may persist buffered writes after process death. For true durability validation, use Docker with LazyFS (see below).

## LazyFS crash testing

LazyFS is a FUSE filesystem that intercepts I/O and maintains its own page cache. Clearing that cache discards unsynced writes, simulating actual power failure.

```bash
# Build the Docker image (includes LazyFS and Kifa)
docker build -f docker/Dockerfile.crash-test -t kifa-crash-test .

# Test cautious mode (expects zero gaps)
docker run --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode cautious

# Test normal mode (allows gaps, up to 49 entries at risk)
docker run --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode normal

# Test emergency mode (expects zero gaps)
docker run --cap-add SYS_ADMIN --device /dev/fuse kifa-crash-test \
  --cycles 10 --flush-mode emergency
```

The `--cap-add SYS_ADMIN --device /dev/fuse` flags are required for FUSE filesystem support. Run with `--help` for all available options.

## Code quality standards

Every pull request must pass these CI gates:

1. **Formatting** — `cargo fc`
2. **Linting** — `cargo cw`
3. **Tests** — `cargo tw` and `cargo twdoc`

Code comments follow these conventions:

- Full sentences, not fragments.
- Third person singular present indicative form.
- Comments explain _why_, not _what_.
- Capitalized at the beginning, ended with a period.
- Comments do not duplicate the code. Good comments do not excuse unclear code.

## Pull request process

1. Fork the repository and create a branch from `master`.
2. Make focused, single-purpose commits.
3. Ensure all CI checks pass locally before pushing.
4. Open a pull request using the existing PR template.
5. One approval from the maintainer is required to merge.

## Security issues

Do not open a public issue for security vulnerabilities. See
[SECURITY.md](SECURITY.md) for the responsible disclosure process.

## License

By submitting a pull request, you agree that your contribution will be licensed
under the same terms as the project: MIT OR Apache-2.0, at the user's choice.
