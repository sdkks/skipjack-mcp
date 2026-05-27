## Summary

<!-- Briefly describe what this PR does and why. -->

## Checklist

- [ ] Pre-commit hook installed: `cp init/pre-commit .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit`
- [ ] `cargo fmt` passes
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo test` passes with no regressions
- [ ] `cargo build --release` succeeds
- [ ] New public types/functions have doc comments
- [ ] New behavior is covered by tests
- [ ] No secrets, API keys, or credentials in the diff

## Test plan

<!-- How did you verify this change? Steps, edge cases, manual testing. -->
