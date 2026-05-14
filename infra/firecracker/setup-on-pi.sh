#!/usr/bin/env bash
# =============================================================================
# Firecracker setup on a Raspberry Pi (4 / 5) running Ubuntu Server 24.04 ARM64.
#
# Mirrors the Lima provisioning script: installs the Firecracker aarch64
# binary and the prebuilt kernel + rootfs into the same paths the
# `app.sandbox.firecracker.*` defaults point at, so the execution-worker
# code requires zero changes.
#
# Pre-requisites:
#   * Pi 4 (8 GB recommended) or Pi 5 with Ubuntu Server 24.04 LTS ARM64 flashed
#   * Booted, SSH'd in, sudo available
#   * Network — needs to reach the Firecracker GitHub releases + S3 CI assets
#
# Run as a regular user (script will sudo where it needs to):
#
#   curl -fsSL <raw-url-to-this-script> | bash
#       # or
#   ./setup-on-pi.sh
#
# After this completes:
#   * /usr/local/bin/firecracker is installed (v1.10.1 aarch64)
#   * /var/lib/firecracker/{vmlinux,rootfs.ext4} are in place
#   * Your user is in the `kvm` group (log out / log in for it to take effect)
#   * `firecracker --version` works
#   * `ls -la /dev/kvm` shows the device
# =============================================================================

set -euo pipefail

FC_VERSION=v1.10.1
ARCH=aarch64

[ "$(uname -m)" = "aarch64" ] || {
  echo "[setup] expected aarch64, got $(uname -m). Are you on a Pi 4/5 with arm64 Linux?"
  exit 1
}

echo "[setup] OS: $(. /etc/os-release; echo "$NAME $VERSION_ID")"
echo "[setup] kernel: $(uname -r)"
echo "[setup] cpu: $(grep -m1 'model name\|Hardware\|Model' /proc/cpuinfo || true)"

# ----- packages -------------------------------------------------------------
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  curl ca-certificates openjdk-17-jdk-headless

# ----- KVM kernel module ---------------------------------------------------
# On Ubuntu 24.04's linux-raspi kernel, KVM is built in / auto-loaded on Pi 4/5.
# This is a defensive check — if /dev/kvm doesn't show up, the Pi's firmware
# probably has virtualization disabled or you're on a stock Raspberry Pi OS
# kernel without KVM support.
if [ ! -c /dev/kvm ]; then
  echo "[setup] /dev/kvm not present yet — trying to load the module"
  sudo modprobe kvm || true
  if [ ! -c /dev/kvm ]; then
    echo "[setup] ERROR: /dev/kvm still missing."
    echo "[setup] Check: /boot/firmware/config.txt should NOT have arm_64bit=0"
    echo "[setup] Check: lscpu | grep -i virtual  ← should mention KVM or VT"
    echo "[setup] Check: dmesg | grep -i kvm     ← look for failure reasons"
    exit 1
  fi
fi

# ----- Firecracker binary --------------------------------------------------
if [ ! -x /usr/local/bin/firecracker ]; then
  echo "[setup] downloading Firecracker $FC_VERSION ($ARCH)"
  cd /tmp
  curl -fsSL -o fc.tgz \
    "https://github.com/firecracker-microvm/firecracker/releases/download/${FC_VERSION}/firecracker-${FC_VERSION}-${ARCH}.tgz"
  tar -xf fc.tgz
  sudo install -m 0755 \
    "release-${FC_VERSION}-${ARCH}/firecracker-${FC_VERSION}-${ARCH}" \
    /usr/local/bin/firecracker
  rm -rf fc.tgz "release-${FC_VERSION}-${ARCH}"
fi

# ----- prebuilt kernel + rootfs --------------------------------------------
sudo install -d /var/lib/firecracker
# The Firecracker CI bumps kernel patch versions silently, so 5.10.223 today
# may be 5.10.225 next month. If the curl below 404s, list the bucket with:
#   curl -s "https://s3.amazonaws.com/spec.ccfc.min/?prefix=firecracker-ci/v1.10/aarch64/" \
#     | grep -oE 'vmlinux-[0-9.]+' | sort -u
# and substitute the latest. v1.10 is pinned because v1.11+ moved to squashfs
# rootfs which would change the Firecracker drive PUT config.
if [ ! -s /var/lib/firecracker/vmlinux ]; then
  echo "[setup] downloading guest kernel"
  sudo curl -fsSL -o /var/lib/firecracker/vmlinux \
    "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/aarch64/vmlinux-5.10.223"
fi
if [ ! -s /var/lib/firecracker/rootfs.ext4 ]; then
  echo "[setup] downloading guest rootfs (~400 MB)"
  sudo curl -fsSL -o /var/lib/firecracker/rootfs.ext4 \
    "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/aarch64/ubuntu-22.04.ext4"
fi

# ----- KVM group membership ------------------------------------------------
if ! getent group kvm >/dev/null; then sudo groupadd kvm; fi
if ! id -nG "$USER" | grep -qw kvm; then
  sudo usermod -aG kvm "$USER"
  echo "[setup] Added $USER to the kvm group — log out and back in (or 'newgrp kvm') for it to take effect."
fi

# ----- summary -------------------------------------------------------------
echo
echo "[setup] ✅ Firecracker ready on $(hostname)"
echo "[setup]   firecracker: $(firecracker --version | head -1)"
echo "[setup]   /dev/kvm:    $(ls -la /dev/kvm)"
echo "[setup]   artifacts:   $(ls -lh /var/lib/firecracker)"
echo
echo "Next: clone the repo, then run the smoke test:"
echo "  git clone <repo> ~/online-judge-at-scale"
echo "  ~/online-judge-at-scale/infra/firecracker/smoke-microvm.sh"
