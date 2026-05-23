#!/usr/bin/env bash
set -euo pipefail

commit_msg="$(cat "$1")"

# Conventional commits: type(optional-scope): description
# SemVer-significant types: feat (minor), fix (patch)
# Other types: docs, style, refactor, perf, test, chore, ci, build, revert
pattern='^(feat|fix|docs|style|refactor|perf|test|chore|ci|build|revert)(\([a-zA-Z0-9_-]+\))?!?: .+'

if ! echo "$commit_msg" | grep -qE "$pattern"; then
    echo "ERROR: Commit message does not follow conventional commits format."
    echo ""
    echo "Expected format: type(scope): description"
    echo ""
    echo "SemVer-significant types:"
    echo "  feat     — new feature (triggers MINOR version bump)"
    echo "  fix      — bug fix (triggers PATCH version bump)"
    echo ""
    echo "Other types:"
    echo "  docs     — documentation only"
    echo "  style    — formatting, missing semicolons, etc."
    echo "  refactor — code change that neither fixes a bug nor adds a feature"
    echo "  perf     — performance improvement"
    echo "  test     — adding or correcting tests"
    echo "  chore    — build process or tooling changes"
    echo "  ci       — CI configuration changes"
    echo "  build    — changes to build system or dependencies"
    echo "  revert   — reverts a previous commit"
    echo ""
    echo "Scope is optional (alphanumeric, hyphens, underscores)."
    echo "Add ! before : for breaking changes (triggers MAJOR version bump)."
    echo ""
    echo "Examples:"
    echo "  feat(daemon): add SIGHUP config reload"
    echo "  fix(cache): correct TTL calculation edge case"
    echo "  feat(api)!: remove deprecated /v1 endpoint"
    echo "  docs: update README with install instructions"
    echo ""
    exit 1
fi
