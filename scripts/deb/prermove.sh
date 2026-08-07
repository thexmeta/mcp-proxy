#!/bin/bash
set -e
# Pre-remove script
# Stop service if running
if command -v systemctl >/dev/null 2>&1; then
    systemctl stop mcp-proxy >/dev/null 2>&1 || true
    systemctl disable mcp-proxy >/dev/null 2>&1 || true
fi
