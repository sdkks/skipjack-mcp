# Contributing to skipjackd

Thanks for your interest in contributing. This document covers the basics.

## Getting started

```bash
git clone https://github.com/sdkks/skipjack-mcp.git
cd skipjack-mcp
cargo build
cargo test
```

## Development workflow

- **Branch:** create a feature branch off `main`
- **Format:** `cargo fmt` before committing (CI enforces it)
- **Lint:** `cargo clippy -- -D warnings` must pass
- **Tests:** `cargo test` must pass with no regressions
- **Commits:** follow [conventional commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `chore:`, etc.)

## Pre-commit hooks

The repo uses a pre-commit hook that runs:

1. `gitleaks protect --staged` — no secrets in commits
2. `cargo fmt --check` — formatting
3. `cargo clippy -- -D warnings` — lint
4. `cargo test` — all tests
5. `cargo build --release` — release build

Install it from the repo root:

```bash
cp init/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Pull requests

- Keep PRs focused — one thing per PR
- Link to any related issues
- Include a test plan in the description
- All checks must pass before merge

## Adding a search provider

See the `Provider` trait in `src/search/provider.rs`. The pattern is:

1. Implement `Provider` for your type
2. Register it in `src/daemon/manager.rs`
3. Add tests with a fixture-based parse test (see existing providers)
4. Add a config section in the provider blocklist in `config.toml`

## Reporting bugs

Open an issue with:

- skipjackd version (`skipjackd --version`)
- Steps to reproduce
- Expected vs actual behavior
- Any relevant logs or error output

## License

MIT — see [LICENSE](LICENSE).
