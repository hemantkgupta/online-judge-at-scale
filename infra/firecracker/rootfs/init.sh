#!/bin/sh
# =============================================================================
# Online Judge — Firecracker guest /init harness.
#
# PID 1 inside the microVM. The actual work — compile, run, measure, return
# a structured verdict — is done by oj-execution-agent (Go binary) listening
# on vsock. This script's only job is to mount /proc + /sys + /tmp, then
# exec the agent.
#
# When the host closes the vsock connection (or the agent process exits for
# any other reason), exec'ing the agent as PID 1 causes the kernel to panic
# with "Attempted to kill init!" — which Firecracker observes and exits.
# That's how the Sandbox Manager learns the VM is done.
# =============================================================================

set -u

mount -t proc  none /proc 2>/dev/null
mount -t sysfs none /sys 2>/dev/null
mount -t tmpfs none /tmp -o size=128M,nodev,nosuid 2>/dev/null

# Export a sane PATH so subprocess spawning from the agent (exec.LookPath)
# can find python3/java/g++ in /usr/bin etc. The kernel passes an empty
# environment to PID 1 unless something explicitly sets these; without PATH,
# Go's exec.LookPath would fall back to the cwd or return ErrNotFound, and
# fork/exec of `python3` would either fail or behave unpredictably.
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HOME=/root
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

# ============================================================================
# Diagnostic dump (rootfs-v4-debug-init). Goes to ttyS0 → captured in
# Firecracker's stdout → /tmp/fc-output.log on the host. Lets us inspect
# the VM's environment + verify python3 / g++ actually run before handing
# control to the agent. Strip this block after the diagnostic is finished.
# ============================================================================
echo ""
echo "=== /init diagnostic dump (v4-debug) ==="
echo "kernel: $(uname -a)"
echo "PATH=$PATH"
echo "HOME=$HOME"
echo ""
echo "-- /usr/bin contents (first 30): --"
ls /usr/bin 2>&1 | head -30
echo ""
echo "-- file /usr/bin/python3 --"
file /usr/bin/python3 2>&1 || echo "(no 'file' command in rootfs; expected ELF symlink)"
ls -la /usr/bin/python3 2>&1
echo ""
echo "-- /usr/bin/python3 -V --"
/usr/bin/python3 -V 2>&1
echo "  exit=$?"
echo ""
echo "-- /usr/bin/python3 -c print --"
/usr/bin/python3 -c "print('hello from init')" 2>&1
echo "  exit=$?"
echo ""
echo "-- /usr/bin/g++ --version --"
/usr/bin/g++ --version 2>&1 | head -2
echo "  exit=$?"
echo ""
echo "-- ld.so.cache check --"
ls -la /etc/ld.so.cache 2>&1
echo ""
echo "-- libpython location --"
find /usr/lib /lib -maxdepth 4 -name 'libpython3*' 2>&1 | head -5
echo ""
echo "-- ldd /usr/bin/python3 --"
ldd /usr/bin/python3 2>&1 | head -10
echo ""
echo "=== /init diagnostic dump end ==="
echo ""

exec /usr/local/bin/oj-execution-agent
