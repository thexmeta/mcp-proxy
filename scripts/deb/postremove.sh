#!/bin/sh
set -e
# Post-remove: forget the unit. Config, state and drop-ins are left in place.
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
