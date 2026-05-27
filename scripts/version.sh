#!/usr/bin/env bash
set -euo pipefail

# version.sh — determine next semver version from conventional commits since the
# last git tag (or all commits if no tags exist).
#
# Output: next version string (e.g., "0.1.1") or empty if no semver commits.
# Exit:   0 always (empty output signals "nothing to bump" to callers).

# Current version from Cargo.toml.
current=$(grep -m1 '^version' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
if [ -z "$current" ]; then
    echo "ERROR: could not parse version from Cargo.toml" >&2
    exit 1
fi

# Commits since last tag (or all commits if no tags).
if last_tag=$(git describe --tags --abbrev=0 2>/dev/null); then
    range="${last_tag}..HEAD"
else
    range="HEAD"
fi

# Gather commit subjects + bodies in the range.
commits=$(git log "$range" --format='%s' 2>/dev/null || true)

if [ -z "$commits" ]; then
    # No commits at all in range — nothing to bump.
    exit 0
fi

# Determine bump level. Highest wins (major > minor > patch).
level=0  # 0=none, 1=patch, 2=minor, 3=major

while IFS= read -r subject; do
    # ---- breaking change ----
    # "type!:" or "type(scope)!:" in subject, or BREAKING CHANGE in body (footer).
    if echo "$subject" | grep -qE '^(feat|fix|docs|style|refactor|perf|test|chore|ci|build|revert)(\([a-zA-Z0-9_-]+\))?!:'; then
        level=3
        break  # major — nothing higher, stop scanning
    fi

    # ---- feat (minor) ----
    if echo "$subject" | grep -qE '^feat(\([a-zA-Z0-9_-]+\))?:'; then
        if [ "$level" -lt 2 ]; then level=2; fi
    fi

    # ---- fix (patch) ----
    if echo "$subject" | grep -qE '^fix(\([a-zA-Z0-9_-]+\))?:'; then
        if [ "$level" -lt 1 ]; then level=1; fi
    fi
done <<< "$commits"

# Also check full commit bodies for BREAKING CHANGE footer.
if [ "$level" -lt 3 ]; then
    bodies=$(git log "$range" --format='%B' 2>/dev/null || true)
    if echo "$bodies" | grep -q 'BREAKING CHANGE'; then
        level=3
    fi
fi

if [ "$level" -eq 0 ]; then
    # No semver-significant commits.
    exit 0
fi

# Compute next version.
IFS='.' read -r major minor patch <<< "$current"
# Strip any pre-release suffix from patch (e.g., "0-rc1" → "0")
patch="${patch%%-*}"

case $level in
    1) patch=$((patch + 1)) ;;
    2) minor=$((minor + 1)); patch=0 ;;
    3) major=$((major + 1)); minor=0; patch=0 ;;
esac

echo "${major}.${minor}.${patch}"
