#!/bin/bash
set -e
# Post-remove script
# Remove user/group if no other packages need them
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
# Note: We don't remove the mcp-proxy user/group here as other packages might depend on them
# They will be cleaned up by the system if truly unused
