#!/usr/bin/env bash
# =============================================================================
# Firecracker smoke test — runs INSIDE the Lima VM provisioned by lima.yaml.
#
# Boots a single microVM through the same REST sequence
# FirecrackerExecutionService.execute(...) uses (boot-source, rootfs drive,
# machine-config, InstanceStart), waits for the guest to come up, then kills
# it. If this script succeeds end-to-end, your Lima setup is correctly wired
# for the FirecrackerExecutionService backend.
#
# Run as the Lima user (not root) — Firecracker doesn't need root, only
# membership in the `kvm` group, which the provision script set up.
#
# Usage (inside the VM):
#   /workspace/infra/firecracker/smoke-microvm.sh
# =============================================================================

set -euo pipefail

FC_BIN=${FC_BIN:-/usr/local/bin/firecracker}
KERNEL=${KERNEL:-/var/lib/firecracker/vmlinux}
ROOTFS=${ROOTFS:-/var/lib/firecracker/rootfs.ext4}
SOCK=${SOCK:-/tmp/fc-smoke-$$.sock}
BOOT_TIMEOUT_SECONDS=${BOOT_TIMEOUT_SECONDS:-15}

# -------- preflight ---------------------------------------------------------
[ -x "$FC_BIN" ]    || { echo "[smoke] firecracker binary not found at $FC_BIN"; exit 1; }
[ -r "$KERNEL" ]    || { echo "[smoke] kernel image not found at $KERNEL"; exit 1; }
[ -r "$ROOTFS" ]    || { echo "[smoke] rootfs image not found at $ROOTFS"; exit 1; }
[ -c /dev/kvm ]     || { echo "[smoke] /dev/kvm missing — KVM not available in this VM"; exit 1; }
[ -r /dev/kvm ] && [ -w /dev/kvm ] \
                    || { echo "[smoke] no rw access to /dev/kvm — add yourself to the kvm group and re-login"; exit 1; }
command -v curl >/dev/null \
                    || { echo "[smoke] curl is required to drive the FC REST socket"; exit 1; }

cleanup() {
  if [ -n "${FC_PID:-}" ] && kill -0 "$FC_PID" 2>/dev/null; then
    kill -9 "$FC_PID" 2>/dev/null || true
  fi
  rm -f "$SOCK"
}
trap cleanup EXIT INT TERM
rm -f "$SOCK"

# -------- launch Firecracker -----------------------------------------------
echo "[smoke] launching $FC_BIN --api-sock $SOCK"
"$FC_BIN" --api-sock "$SOCK" >/tmp/fc-smoke.log 2>&1 &
FC_PID=$!

# Wait for the API socket to come up (max 2s).
for _ in $(seq 1 40); do
  [ -S "$SOCK" ] && break
  sleep 0.05
done
[ -S "$SOCK" ] || { echo "[smoke] FC API socket never appeared. logs:"; cat /tmp/fc-smoke.log; exit 1; }
echo "[smoke] FC api-sock up (pid=$FC_PID)"

# -------- helper for FC REST API over the Unix socket ----------------------
fc_put() {
  local path=$1 body=$2
  curl -sS --unix-socket "$SOCK" \
       -X PUT "http://localhost${path}" \
       -H 'Accept: application/json' -H 'Content-Type: application/json' \
       -d "$body" \
       -w '\n[smoke] PUT %{http_code} '"${path}"'\n'
}

# -------- configure the microVM --------------------------------------------
# Kernel command line:
#   * console=ttyS0 — print boot log to the serial console (FC stdout)
#   * reboot=k panic=1 — clean shutdown semantics for the smoke test
#   * pci=off — Firecracker doesn't emulate PCI
#   * On aarch64, init=/sbin/init is fine for the upstream rootfs image.
fc_put /boot-source '{
  "kernel_image_path": "'"$KERNEL"'",
  "boot_args": "console=ttyS0 reboot=k panic=1 pci=off"
}'

fc_put /drives/rootfs '{
  "drive_id": "rootfs",
  "path_on_host": "'"$ROOTFS"'",
  "is_root_device": true,
  "is_read_only": true
}'

fc_put /machine-config '{
  "vcpu_count": 1,
  "mem_size_mib": 256
}'

# -------- start the VM -----------------------------------------------------
echo "[smoke] starting microVM..."
fc_put /actions '{"action_type": "InstanceStart"}'

# -------- wait for the guest to come up ------------------------------------
# We tail the FC stdout for a boot marker. The Ubuntu rootfs prints
# `cloud-init` and `login:` lines once userspace is up — either is fine.
echo "[smoke] waiting up to ${BOOT_TIMEOUT_SECONDS}s for guest userspace..."
DEADLINE=$(( $(date +%s) + BOOT_TIMEOUT_SECONDS ))
BOOTED=no
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  if grep -qE 'login:|cloud-init|Welcome to Ubuntu' /tmp/fc-smoke.log 2>/dev/null; then
    BOOTED=yes
    break
  fi
  sleep 0.25
done

if [ "$BOOTED" = "yes" ]; then
  echo
  echo "[smoke] ✅ microVM booted to userspace. Killing it now."
  echo "[smoke] FC log tail:"
  tail -n 15 /tmp/fc-smoke.log | sed 's/^/    /'
else
  echo
  echo "[smoke] ❌ microVM did not reach userspace within ${BOOT_TIMEOUT_SECONDS}s."
  echo "[smoke] FC log:"
  sed 's/^/    /' /tmp/fc-smoke.log
  exit 1
fi
