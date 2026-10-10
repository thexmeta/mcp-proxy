#!/bin/sh
set -e
# Post-install: register the unit. The service is enabled but not started,
# because it needs a config first.
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    systemctl enable mcp-proxy >/dev/null 2>&1 || true
fi
echo "mcp-proxy installed."
echo "Config: /etc/mcp-proxy/config.toml (example: /usr/share/doc/mcp-proxy/config.example.toml)"
echo "Site settings: /etc/systemd/system/mcp-proxy.service.d/*.conf"
