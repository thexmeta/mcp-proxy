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

# Create deb scripts directory
mkdir -p "${PROJECT_ROOT}/scripts/deb"

# Build the project in release mode
echo "=== Building Rust project in release mode ==="
cd "${PROJECT_ROOT}"
cargo generate-lockfile
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

# Create deb scripts
cat > "${PROJECT_ROOT}/scripts/deb/preinstall.sh" << 'EOF'
#!/bin/bash
set -e
# Pre-install script
# Create config directory for mxadm user
mkdir -p /home/mxadm/.mcp-proxy
chown mxadm:mxadm /home/mxadm/.mcp-proxy
EOF

cat > "${PROJECT_ROOT}/scripts/deb/postinstall.sh" << 'EOF'
#!/bin/bash
set -e
# Post-install script
# Reload systemd if needed
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    systemctl enable mcp-proxy >/dev/null 2>&1 || true
fi
# Set proper permissions on config file
if [[ -f /home/mxadm/.mcp-proxy/config.toml ]]; then
    chown mxadm:mxadm /home/mxadm/.mcp-proxy/config.toml
    chmod 640 /home/mxadm/.mcp-proxy/config.toml
fi
echo "mcp-proxy installed successfully!"
echo "Configuration file: /home/mxadm/.mcp-proxy/config.toml"
echo "Run 'mcp-proxy --help' for usage information."
EOF

cat > "${PROJECT_ROOT}/scripts/deb/prermove.sh" << 'EOF'
#!/bin/bash
set -e
# Pre-remove script
# Stop service if running
if command -v systemctl >/dev/null 2>&1; then
    systemctl stop mcp-proxy >/dev/null 2>&1 || true
    systemctl disable mcp-proxy >/dev/null 2>&1 || true
fi
EOF

cat > "${PROJECT_ROOT}/scripts/deb/postremove.sh" << 'EOF'
#!/bin/bash
set -e
# Post-remove script
# Remove user/group if no other packages need them
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
# Note: We don't remove the mcp-proxy user/group here as other packages might depend on them
# They will be cleaned up by the system if truly unused
EOF

chmod +x "${PROJECT_ROOT}/scripts/deb/"*.sh

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
    echo "To verify: dpkg-deb --extract ${DEB_FILE} /tmp/test && /tmp/test/usr/bin/mcp-proxy --version"
else
    echo "ERROR: Package creation failed"
    exit 1
fi

echo "=== Build complete ==="