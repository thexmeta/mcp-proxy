#!/bin/bash
set -euo pipefail

# Build script for creating Debian package (.deb) for mcp-proxy
# Uses nfpm for packaging

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DIST_DIR="${PROJECT_ROOT}/dist"

echo "=== Building mcp-proxy Debian package ==="
echo "Project root: ${PROJECT_ROOT}"
echo "Dist directory: ${DIST_DIR}"

# Create dist directory
mkdir -p "${DIST_DIR}"

# MSRV fail-fast guard: Cargo.toml rust-version is the single source of truth.
MIN_RUSTC="1.97"  # keep in sync with `rust-version` in Cargo.toml
MIN_MAJOR="${MIN_RUSTC%%.*}"
MIN_MINOR="${MIN_RUSTC#*.}"
ACTIVE_RUSTC="$(rustc --version | awk '{print $2}')"
ACTIVE_NUM="${ACTIVE_RUSTC%%-*}"  # strip pre-release suffixes (e.g. -nightly)
ACTIVE_MAJOR="${ACTIVE_NUM%%.*}"
ACTIVE_REST="${ACTIVE_NUM#*.}"
ACTIVE_MINOR="${ACTIVE_REST%%.*}"
if ! [[ "${ACTIVE_MAJOR}" =~ ^[0-9]+$ && "${ACTIVE_MINOR}" =~ ^[0-9]+$ ]] \
    || [[ "${ACTIVE_MAJOR}" -lt "${MIN_MAJOR}" ]] \
    || { [[ "${ACTIVE_MAJOR}" -eq "${MIN_MAJOR}" ]] && [[ "${ACTIVE_MINOR}" -lt "${MIN_MINOR}" ]]; }; then
    echo "ERROR: mcp-proxy requires rustc >= ${MIN_RUSTC} (Cargo.toml rust-version), but active rustc is ${ACTIVE_RUSTC}."
    echo "Fix: rustup toolchain install 1.97, then select it (rustup override set 1.97, rust-toolchain.toml pin),"
    echo "     or run once with the escape hatch: RUSTUP_TOOLCHAIN=1.97 scripts/build-deb.sh"
    exit 1
fi
echo "rustc ${ACTIVE_RUSTC} satisfies MSRV >= ${MIN_RUSTC}"

# Build the project in release mode
echo "=== Building Rust project in release mode ==="
cd "${PROJECT_ROOT}"
cargo build --release --locked --features sqlite-cache

# Verify binary exists
BINARY_PATH="${PROJECT_ROOT}/target/release/mcp-proxy"
if [[ ! -f "${BINARY_PATH}" ]]; then
    echo "ERROR: Binary not found at ${BINARY_PATH}"
    exit 1
fi

echo "Binary found: ${BINARY_PATH}"
# Verify binary works (no --version flag, so use --help to verify)
"${BINARY_PATH}" --help >/dev/null 2>&1 && echo "Binary verified successfully"

# Extract version from Cargo.toml and generate nfpm.yaml from template
VERSION=$(grep '^version = ' "${PROJECT_ROOT}/Cargo.toml" | head -1 | sed 's/version = "\(.*\)"/\1/')
export VERSION
echo "=== Package version: ${VERSION} ==="

# Generate nfpm.yaml from template (nfpm.yaml uses ${VERSION} placeholder)
envsubst '${VERSION}' < "${PROJECT_ROOT}/nfpm.yaml" > "${PROJECT_ROOT}/nfpm.generated.yaml"

# Build the Debian package using nfpm
echo "=== Building Debian package with nfpm ==="
cd "${PROJECT_ROOT}"
nfpm package --packager deb --target "${DIST_DIR}" --config nfpm.generated.yaml

# Verify the package
DEB_FILE=$(ls -1 "${DIST_DIR}"/*.deb 2>/dev/null | head -1)
if [[ -f "${DEB_FILE}" ]]; then
    echo "=== Package created successfully ==="
    echo "Package: ${DEB_FILE}"
    echo ""
    echo "Package info:"
    dpkg-deb -I "${DEB_FILE}"
    echo ""
    echo "Package contents:"
    dpkg-deb -c "${DEB_FILE}"
    echo ""
    echo "To install: sudo dpkg -i ${DEB_FILE}"
    echo "To verify: dpkg-deb --extract ${DEB_FILE} /tmp/test && /tmp/test/usr/bin/mcp-proxy --help"
else
    echo "ERROR: Package creation failed"
    exit 1
fi

echo "=== Build complete ==="