#!/bin/sh
set -e
# Pre-remove: stop and disable the service when the package goes away.
# dpkg also runs this on upgrade ("upgrade", "failed-upgrade"); the service is
# left alone then, and postinstall restarts it on the new binary.
case "${1:-}" in
    remove|deconfigure)
        if command -v systemctl >/dev/null 2>&1; then
            systemctl stop mcp-proxy >/dev/null 2>&1 || true
            systemctl disable mcp-proxy >/dev/null 2>&1 || true
        fi
        ;;
esac
