#!/usr/bin/env bash
set -euo pipefail

echo "==> gitleaks protect --staged"
if command -v gitleaks &>/dev/null; then
    gitleaks protect --staged --verbose
else
    echo "gitleaks not installed — skipping (install: brew install gitleaks)"
fi

echo "==> cargo fmt --check"
cargo fmt --check

echo "==> cargo clippy -- -D warnings"
cargo clippy -- -D warnings

echo "==> cargo test"
cargo test

echo "==> cargo build --release"
cargo build --release
