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

exec /usr/local/bin/oj-execution-agent
