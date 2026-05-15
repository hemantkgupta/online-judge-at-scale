#!/usr/bin/env bash
# =============================================================================
# Build a Firecracker harness rootfs for the online-judge worker.
#
# Produces an ext4 image at $OUT (default /var/lib/firecracker/rootfs.ext4)
# containing:
#   * Ubuntu 22.04 minimal base    (via debootstrap)
#   * python3, openjdk-21-jdk-headless, gcc, g++   (language runtimes)
#   * busybox + coreutils + util-linux  (timeout, head, sync, poweroff, mount)
#   * /init  — our PID-1 harness script (see init.sh in this directory)
#
# Run as root on a Linux host with debootstrap + e2fsprogs installed. The
# Stage 4 startup script on the compute VM invokes this on first boot.
# Idempotent: if $OUT already exists AND has the OJ_HARNESS_VERSION marker,
# we skip the rebuild.
#
# Usage:
#   sudo ./build-rootfs.sh                 # writes /var/lib/firecracker/rootfs.ext4
#   sudo OUT=/tmp/rootfs.ext4 ./build-rootfs.sh
#
# Why "rebuild on the host instead of shipping a pre-baked image"?
#   * The image is ~600 MB — too big to commit, and changes with every
#     language version bump. Pinning the Ubuntu suite gives reproducibility.
#   * Storing it on the persistent disk means it survives VM restarts; the
#     idempotency check means we rebuild only when the harness version changes.
#   * No GCS bucket needed for artifacts.
# =============================================================================

set -euo pipefail

OUT="${OUT:-/var/lib/firecracker/rootfs.ext4}"
SIZE_MB="${SIZE_MB:-1024}"        # 1 GiB — fits JDK + g++ + Python with headroom
SUITE="${SUITE:-jammy}"            # Ubuntu 22.04 LTS
MIRROR="${MIRROR:-http://archive.ubuntu.com/ubuntu}"

# Bump this whenever init.sh or the Go agent change.
OJ_HARNESS_VERSION="oj-rootfs-v3-pertest-agent"

HERE="$(cd "$(dirname "$0")" && pwd)"
INIT_SRC="$HERE/init.sh"
# Path to the Go agent source tree. Built on the host with `go build` and
# the static binary is dropped into the rootfs at /usr/local/bin.
AGENT_SRC_DIR="${AGENT_SRC_DIR:-$HERE/../agent}"

# ---------- preflight -------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
    echo "[build-rootfs] must run as root (need chroot, mke2fs, debootstrap)" >&2
    exit 1
fi
[ -r "$INIT_SRC" ] || { echo "[build-rootfs] missing $INIT_SRC" >&2; exit 1; }

# Idempotency: skip if existing image has the correct version marker.
if [ -s "$OUT" ]; then
    # debugfs without root would also work; we're root here so just check the
    # version file embedded in the image.
    EXISTING="$(debugfs -R 'cat /etc/oj-rootfs-version' "$OUT" 2>/dev/null | tr -d '[:space:]' || true)"
    if [ "$EXISTING" = "$OJ_HARNESS_VERSION" ]; then
        echo "[build-rootfs] $OUT is already at $OJ_HARNESS_VERSION — skipping"
        exit 0
    fi
    echo "[build-rootfs] existing $OUT is '$EXISTING' (want $OJ_HARNESS_VERSION) — rebuilding"
fi

# ---------- install build deps if missing -----------------------------------
need_pkgs=()
for cmd in debootstrap mke2fs debugfs go; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        case "$cmd" in
            debootstrap) need_pkgs+=("debootstrap") ;;
            mke2fs|debugfs) need_pkgs+=("e2fsprogs") ;;
            go) need_pkgs+=("golang-go") ;;
        esac
    fi
done
if [ "${#need_pkgs[@]}" -gt 0 ]; then
    echo "[build-rootfs] apt-get install ${need_pkgs[*]}"
    DEBIAN_FRONTEND=noninteractive apt-get update -y
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${need_pkgs[@]}"
fi

# ---------- build the Execution Agent (Go static binary) --------------------
# Compiled on the host (we're already on the compute VM which is the same
# linux/amd64 architecture as the guest), then dropped into the staged
# rootfs. The agent is the only thing inside the VM that talks to the
# outside world (via vsock), so it must be tiny and statically linked —
# the guest rootfs has no shared libraries to dynamic-load against.
[ -d "$AGENT_SRC_DIR" ] || { echo "[build-rootfs] missing agent source dir $AGENT_SRC_DIR" >&2; exit 1; }

AGENT_BUILD_DIR="$(mktemp -d /tmp/oj-agent-build.XXXXXX)"
# Go needs a writable home for its module cache + build cache. When
# build-rootfs.sh runs from the GCE startup-script systemd service, $HOME
# is empty / unset, so Go errors out with "module cache not found".
# Pin GOPATH + GOCACHE explicitly under /tmp so the build always has a
# writable workspace and dependencies (mdlayher/vsock + transitives)
# can be downloaded into it.
export GOPATH="$AGENT_BUILD_DIR/gopath"
export GOCACHE="$AGENT_BUILD_DIR/gocache"
mkdir -p "$GOPATH" "$GOCACHE"

echo "[build-rootfs] building Go execution agent into $AGENT_BUILD_DIR"
(
    cd "$AGENT_SRC_DIR"
    # CGO_ENABLED=0 → fully static (no libc.so). GOOS=linux GOARCH=amd64
    # is the only target Firecracker x86_64 boots; bake them in for
    # reproducibility even if the host already matches.
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build -trimpath -ldflags="-s -w" \
        -o "$AGENT_BUILD_DIR/oj-execution-agent" ./cmd/agent
)
echo "[build-rootfs] agent built: $(du -h "$AGENT_BUILD_DIR/oj-execution-agent" | cut -f1)"

# ---------- stage the rootfs in a temp dir ----------------------------------
STAGE="$(mktemp -d /tmp/oj-rootfs.XXXXXX)"
trap 'set +e; umount "$STAGE/proc" "$STAGE/sys" "$STAGE/dev" 2>/dev/null; rm -rf "$STAGE"' EXIT

echo "[build-rootfs] debootstrap $SUITE → $STAGE"
debootstrap --variant=minbase --include=ca-certificates "$SUITE" "$STAGE" "$MIRROR"

# ---------- second-stage install: language runtimes + tiny init's friends --
# We mount /proc + /dev into the chroot so apt-get's post-install scripts
# (especially for openjdk's update-alternatives) behave.
mount --bind /proc "$STAGE/proc"
mount --bind /sys  "$STAGE/sys"
mount --bind /dev  "$STAGE/dev"

cat > "$STAGE/tmp/second-stage.sh" <<'CHROOT'
#!/bin/sh
set -eu
export DEBIAN_FRONTEND=noninteractive

# Add universe (for openjdk-21 on jammy via Ubuntu Backports archive).
echo "deb http://archive.ubuntu.com/ubuntu jammy main universe"           >  /etc/apt/sources.list
echo "deb http://archive.ubuntu.com/ubuntu jammy-updates main universe"   >> /etc/apt/sources.list
echo "deb http://archive.ubuntu.com/ubuntu jammy-security main universe"  >> /etc/apt/sources.list
echo "deb http://archive.ubuntu.com/ubuntu jammy-backports main universe" >> /etc/apt/sources.list

apt-get update -y

apt-get install -y --no-install-recommends \
    busybox-static \
    coreutils \
    util-linux \
    python3 \
    openjdk-21-jdk-headless \
    g++ \
    libc6-dev \
    libstdc++-11-dev

# Strip apt + locale caches so the rootfs stays under 1 GiB.
apt-get clean
rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
       /usr/share/locale/!(en|en_US|C.UTF-8) 2>/dev/null || true

# Mount points the /init script needs.
mkdir -p /code /proc /sys /tmp
CHROOT

chmod +x "$STAGE/tmp/second-stage.sh"
chroot "$STAGE" /tmp/second-stage.sh
rm -f "$STAGE/tmp/second-stage.sh"

# ---------- drop the Execution Agent + /init harness -----------------------
# /init is a 5-line shell that mounts proc/sys, then exec's the agent.
# The agent listens on vsock; when the host closes its connection (or the
# job finishes), the agent exits, PID 1 dies, the kernel panics, and
# Firecracker shuts the VM down. That's how the host learns the VM is done.
install -m 0755 "$AGENT_BUILD_DIR/oj-execution-agent" "$STAGE/usr/local/bin/oj-execution-agent"
install -m 0755 "$INIT_SRC" "$STAGE/init"

# Cleanup builder workdir; the binary is already copied into the staged rootfs.
rm -rf "$AGENT_BUILD_DIR"

# Make absolutely sure poweroff is present (the agent doesn't call it, but
# operators ssh-ing into a VM for debugging will appreciate it).
ln -sf /bin/busybox "$STAGE/usr/bin/poweroff"  2>/dev/null || true

# Version marker — used by the idempotency check at the top of this script.
echo "$OJ_HARNESS_VERSION" > "$STAGE/etc/oj-rootfs-version"

# Tear down the bind mounts before packing.
umount "$STAGE/proc" "$STAGE/sys" "$STAGE/dev"
trap 'set +e; rm -rf "$STAGE"' EXIT

# ---------- pack as an ext4 image -------------------------------------------
echo "[build-rootfs] packing into ext4 image ($SIZE_MB MiB) → $OUT"
mkdir -p "$(dirname "$OUT")"
TMP_IMG="$OUT.partial"
rm -f "$TMP_IMG"

# `mke2fs -d <staging>` populates the FS at format time — no mount required.
# -F: force formatting a regular file. -E nodiscard: skip the trim that adds
# minutes on a fresh GCP pd-balanced disk for no real benefit.
mke2fs -t ext4 -F -E nodiscard -d "$STAGE" -L oj-rootfs \
       "$TMP_IMG" "${SIZE_MB}M"

mv "$TMP_IMG" "$OUT"

echo "[build-rootfs] done. $OUT  ($(du -h "$OUT" | cut -f1))"
echo "[build-rootfs] version marker: $OJ_HARNESS_VERSION"
