#!/bin/sh
# Checks the Debian maintainer scripts against the arguments dpkg passes them.
#
# Why: dpkg runs preremove with "upgrade" during every upgrade. If the scripts
# stop or disable the service there, every upgrade leaves the proxy down (and
# flips the admin's enabled/disabled choice). Each case below pins which
# systemctl calls a given dpkg step may make.
#
# A fake systemctl first on PATH records the calls; the real system is never
# touched. Usage: sh scripts/deb/test-hooks.sh   (exit 0 = all pass)
set -u

here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
# Only removes the scratch dir this script just created.
trap 'rm -rf "$tmp"' EXIT

log="$tmp/systemctl.log"
# shellcheck disable=SC2016 # the fake must expand $* when it runs, not here
printf '#!/bin/sh\necho "$*" >> "$SYSTEMCTL_LOG"\n' > "$tmp/systemctl"
chmod +x "$tmp/systemctl"

pass=0
fail=0

# check <script> <expected calls joined by ';'> [dpkg args...]
check() {
    script=$1
    want=$2
    shift 2
    : > "$log"
    PATH="$tmp:$PATH" SYSTEMCTL_LOG="$log" sh "$here/$script" "$@" >/dev/null 2>&1
    rc=$?
    got=$(tr '\n' ';' < "$log")
    if [ "$rc" -eq 0 ] && [ "$got" = "$want" ]; then
        pass=$((pass + 1))
        echo "ok   $script $*"
    else
        fail=$((fail + 1))
        echo "FAIL $script $*"
        echo "     exit: $rc"
        echo "     want: '$want'"
        echo "     got:  '$got'"
    fi
}

# Removal: the service must stop and be disabled.
check preremove.sh 'stop mcp-proxy;disable mcp-proxy;' remove
check preremove.sh 'stop mcp-proxy;disable mcp-proxy;' deconfigure in-favour other 1.0 removing mcp-proxy 1.4.2~b3-2
# Upgrade: the old package's preremove must leave the service alone.
check preremove.sh '' upgrade 1.4.2~b3-3
check preremove.sh '' failed-upgrade 1.4.2~b3-1

# First install (no previous version): enable, do not start (needs a config).
check postinstall.sh 'daemon-reload;enable mcp-proxy;' configure
check postinstall.sh 'daemon-reload;enable mcp-proxy;' configure ''
# Upgrade: restart only if running, keep the enabled/disabled choice.
check postinstall.sh 'daemon-reload;try-restart mcp-proxy;' configure 1.4.2~b3-1
# Error unwinding: no enable, no restart.
check postinstall.sh 'daemon-reload;' abort-upgrade 1.4.2~b3-2

check postremove.sh 'daemon-reload;' remove
check postremove.sh 'daemon-reload;' upgrade 1.4.2~b3-2

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
