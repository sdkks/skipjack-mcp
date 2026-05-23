#!/usr/bin/env bash
set -euo pipefail

# install.sh — install metasearchd from GitHub releases
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/said/ocak-forge/main/install.sh | sh
#   VERSION=0.1.0 sh install.sh
#
# Detects OS and architecture, downloads the matching release binary,
# and installs to ~/.local/bin (or /usr/local/bin with sudo).

REPO="${REPO:-said/metasearchd}"
VERSION="${VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# ANSI colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

log_info()  { printf "${GREEN}[INFO]${NC} %s\n" "$*"; }
log_error() { printf "${RED}[ERROR]${NC} %s\n" "$*" >&2; }

# --- Detect OS and arch ------------------------------------------------
detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *)
            log_error "Unsupported OS: $(uname -s)"
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64)  arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        *)
            log_error "Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac

    echo "${os}-${arch}"
}

# --- Main ---------------------------------------------------------------
main() {
    local platform binary_name tarball_url tarball_name

    platform="$(detect_platform)"
    binary_name="metasearchd-${platform}"
    tarball_name="${binary_name}.tar.gz"

    log_info "Detected platform: ${platform}"
    log_info "Installing metasearchd ${VERSION} to ${INSTALL_DIR}"

    # Determine download URL
    if [ "${VERSION}" = "latest" ]; then
        tarball_url="https://github.com/${REPO}/releases/latest/download/${tarball_name}"
    else
        tarball_url="https://github.com/${REPO}/releases/download/${VERSION}/${tarball_name}"
    fi

    # Create temp directory
    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT

    log_info "Downloading ${tarball_url} ..."
    if command -v curl > /dev/null 2>&1; then
        curl -fsSL -o "${tmpdir}/${tarball_name}" "${tarball_url}"
    elif command -v wget > /dev/null 2>&1; then
        wget -q -O "${tmpdir}/${tarball_name}" "${tarball_url}"
    else
        log_error "Neither curl nor wget found. Please install one and retry."
        exit 1
    fi

    log_info "Extracting ..."
    tar xzf "${tmpdir}/${tarball_name}" -C "${tmpdir}"

    mkdir -p "${INSTALL_DIR}"
    cp "${tmpdir}/metasearchd" "${INSTALL_DIR}/metasearchd"
    chmod +x "${INSTALL_DIR}/metasearchd"

    log_info "metasearchd installed to ${INSTALL_DIR}/metasearchd"

    # Verify installation
    if command -v metasearchd > /dev/null 2>&1; then
        metasearchd --version 2>&1 || true
    elif [ -x "${INSTALL_DIR}/metasearchd" ]; then
        "${INSTALL_DIR}/metasearchd" --version 2>&1 || true
    fi

    log_info "Installation complete. Ensure ${INSTALL_DIR} is in your PATH."
}

main "$@"
