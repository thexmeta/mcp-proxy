#!/bin/sh
set -e
# Post-install: register the unit. dpkg calls "configure <old-version>";
# old-version is empty on a first install.
# First install: enable but do not start, because it needs a config first.
# Upgrade: restart only if it was running; keep the enabled/disabled choice.
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    if [ "${1:-}" = configure ]; then
        if [ -z "${2:-}" ]; then
            systemctl enable mcp-proxy >/dev/null 2>&1 || true
        else
            systemctl try-restart mcp-proxy >/dev/null 2>&1 || true
        fi
    fi
fi
echo "mcp-proxy installed."
echo "Config: /etc/mcp-proxy/config.toml (example: /usr/share/doc/mcp-proxy/config.example.toml)"
echo "Site settings: /etc/systemd/system/mcp-proxy.service.d/*.conf"
