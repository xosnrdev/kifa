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

```sh
git clone https://github.com/xosnrdev/kifa.git
cd kifa
cargo build --workspace
cargo test --workspace
```

To run the full CI check locally before pushing:

```sh
cargo fmt --all -- --check
cargo clippy --workspace -- --deny warnings -W clippy::pedantic
cargo test --workspace
```

## Code quality standards

Every pull request must pass these CI gates:

1. **Formatting** — `cargo fmt --all -- --check`
2. **Linting** — `cargo clippy --workspace -- --deny warnings -W clippy::pedantic`
3. **Tests** — `cargo test --workspace`

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
