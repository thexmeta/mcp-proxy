#!/bin/bash
set -e
# Pre-install script
# Create mcp-proxy user/group if they don't exist
if ! getent group mcp-proxy >/dev/null; then
    addgroup --system mcp-proxy
fi
if ! getent passwd mcp-proxy >/dev/null; then
    adduser --system --ingroup mcp-proxy --home /var/lib/mcp-proxy --shell /usr/sbin/nologin mcp-proxy
fi
# Create config directory
mkdir -p /var/lib/mcp-proxy/.mcp-proxy
mkdir -p /var/lib/mcp-proxy
mkdir -p /var/log/mcp-proxy
chown mcp-proxy:mcp-proxy /var/lib/mcp-proxy /var/log/mcp-proxy
chown mcp-proxy:mcp-proxy /var/lib/mcp-proxy/.mcp-proxy
