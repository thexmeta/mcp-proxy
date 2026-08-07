#!/bin/bash
set -e
# Pre-install script
# Create config directory for mxadm user
mkdir -p /home/mxadm/.mcp-proxy
chown mxadm:mxadm /home/mxadm/.mcp-proxy
