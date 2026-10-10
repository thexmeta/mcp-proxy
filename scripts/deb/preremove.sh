#!/bin/sh
set -e
# Pre-remove: stop and disable the service.
if command -v systemctl >/dev/null 2>&1; then
    systemctl stop mcp-proxy >/dev/null 2>&1 || true
    systemctl disable mcp-proxy >/dev/null 2>&1 || true
fi
