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
