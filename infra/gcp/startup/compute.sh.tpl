#!/usr/bin/env bash
# =============================================================================
# compute startup script (terraform templatefile target).
#
# Runs on every boot of oj-compute. Installs the host-side tooling the three
# sandbox backends need:
#   * Docker engine             — for the Docker + gVisor backends
#   * Firecracker v1.10.1 + kernel + rootfs  — for the Firecracker backend
#   * gVisor (runsc)            — registered as a Docker runtime
#   * Seccomp-BPF profile       — used by the Linux-hardening Docker variant
#
# Then drops compose YAML + .env + a systemd unit that brings up the
# execution-worker container.
#
# Template variables:
#   ${ar_url}                 e.g. asia-south1-docker.pkg.dev/online-judge-hk/oj-images
#   ${region}                 e.g. asia-south1
#   ${control_plane_ip}       e.g. 10.0.0.2 (where Kafka/CRDB live)
#   ${compose_yaml_b64}       base64 of compute-compose.yml
#   ${seccomp_profile_b64}    base64 of infra/seccomp/sandbox-seccomp.json
#   ${rootfs_init_b64}        base64 of infra/firecracker/rootfs/init.sh
#   ${rootfs_builder_b64}     base64 of infra/firecracker/rootfs/build-rootfs.sh
#   ${sandbox_backend}        docker | firecracker
#   ${sandbox_docker_runtime} runc | runsc
#   ${linux_hardening_enabled} true | false
# =============================================================================

set -euo pipefail

LOG=/var/log/oj-startup.log
exec > >(tee -a "$LOG") 2>&1
echo "[oj-startup] $(date -Iseconds) — compute.sh begin"

# Pinned versions. Bump deliberately, not by accident.
FIRECRACKER_VERSION="v1.10.1"
FIRECRACKER_CI_BASE="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/x86_64"
GVISOR_RELEASE="release-20240826.0"

# ---------- Docker install (idempotent) -------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  echo "[oj-startup] installing docker engine + compose plugin"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg lsb-release jq
  install -m 0755 -d /etc/apt/keyrings
  # --batch --no-tty: gpg 2.x in systemd-run startup scripts has no
  # controlling terminal; without these flags it tries to open /dev/tty
  # for status messages and dies with "cannot open '/dev/tty'".
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    gpg --batch --no-tty --yes --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
  usermod -aG docker hemant || true
fi

# ---------- gcloud (for AR docker auth) -------------------------------------
if ! command -v gcloud >/dev/null 2>&1; then
  echo "[oj-startup] installing google-cloud-cli"
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --batch --no-tty --yes --dearmor -o /usr/share/keyrings/cloud.google.gpg
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list
  apt-get update -y
  apt-get install -y google-cloud-cli
fi

AR_REGISTRY="$(echo '${ar_url}' | cut -d/ -f1)"
gcloud auth configure-docker "$AR_REGISTRY" --quiet

# ---------- Firecracker binary + kernel + rootfs ----------------------------
# /dev/kvm must exist (provided by nested-virt enabled on this VM). Add the
# `hemant` user + `docker` system user to the kvm group so privileged
# containers can open it.
if [[ ! -e /dev/kvm ]]; then
  echo "[oj-startup] FATAL: /dev/kvm missing — nested virt not enabled?" >&2
  exit 1
fi
groupadd -f kvm
chgrp kvm /dev/kvm
chmod g+rw /dev/kvm
usermod -aG kvm hemant || true

install -d -m 0755 /usr/local/bin /var/lib/firecracker

if [[ ! -x /usr/local/bin/firecracker ]]; then
  echo "[oj-startup] downloading firecracker $FIRECRACKER_VERSION"
  TMP="$(mktemp -d)"
  curl -fsSL -o "$TMP/fc.tgz" \
    "https://github.com/firecracker-microvm/firecracker/releases/download/$FIRECRACKER_VERSION/firecracker-$FIRECRACKER_VERSION-x86_64.tgz"
  tar -xzf "$TMP/fc.tgz" -C "$TMP"
  install -m 0755 "$TMP/release-$FIRECRACKER_VERSION-x86_64/firecracker-$FIRECRACKER_VERSION-x86_64" \
    /usr/local/bin/firecracker
  rm -rf "$TMP"
fi
echo "[oj-startup] firecracker: $(/usr/local/bin/firecracker --version | head -1)"

# Guest kernel from the Firecracker CI bucket — generic, no customization
# needed (we only swap the rootfs). Kernel files live directly under
# v<X.Y>/<arch>/ in the CI bucket (no /kernels/ subdir, despite what stale
# docs may suggest).
if [[ ! -s /var/lib/firecracker/vmlinux ]]; then
  echo "[oj-startup] downloading guest kernel (vmlinux-5.10.223)"
  curl -fsSL -o /var/lib/firecracker/vmlinux \
    "$FIRECRACKER_CI_BASE/vmlinux-5.10.223"
fi

# ---------- Custom OJ harness rootfs (replaces the generic CI rootfs) -------
# The CI Ubuntu rootfs has no /init harness — it boots systemd and stops
# there. For real code execution we need a rootfs whose PID 1 mounts the
# code drive, runs the contestant program, writes output, and powers off.
# build-rootfs.sh produces that.
echo "[oj-startup] installing rootfs build deps (debootstrap, e2fsprogs, unzip)"
apt-get install -y --no-install-recommends debootstrap e2fsprogs unzip

install -d -m 0755 /opt/oj/rootfs-builder
echo '${rootfs_init_b64}'    | base64 -d > /opt/oj/rootfs-builder/init.sh
echo '${rootfs_builder_b64}' | base64 -d > /opt/oj/rootfs-builder/build-rootfs.sh
chmod +x /opt/oj/rootfs-builder/init.sh /opt/oj/rootfs-builder/build-rootfs.sh

# Unzip the Execution Agent Go source tree into /opt/oj/agent/ so
# build-rootfs.sh can find it and compile the in-guest binary. The
# tarball is injected as base64 by terraform via the archive_file data
# source over the in-repo infra/firecracker/agent/ directory.
echo "[oj-startup] staging Execution Agent source for build-rootfs"
install -d -m 0755 /opt/oj/agent
echo '${agent_src_zip_b64}' | base64 -d > /tmp/oj-agent-src.zip
unzip -q -o /tmp/oj-agent-src.zip -d /opt/oj/agent
rm -f /tmp/oj-agent-src.zip

# Run the builder. It self-skips if the existing rootfs already has the
# correct version marker, so re-running compute.sh on subsequent boots is
# a no-op once the rootfs is built. Point build-rootfs.sh at the unzipped
# agent source via the AGENT_SRC_DIR env var.
echo "[oj-startup] building harness rootfs (first time ~10 min, then cached)"
AGENT_SRC_DIR=/opt/oj/agent /opt/oj/rootfs-builder/build-rootfs.sh

# ---------- gVisor (runsc) registered as a Docker runtime -------------------
if ! command -v runsc >/dev/null 2>&1; then
  echo "[oj-startup] installing gvisor ($GVISOR_RELEASE)"
  ARCH="$(uname -m)"
  TMP="$(mktemp -d)"
  for f in runsc containerd-shim-runsc-v1; do
    curl -fsSL -o "$TMP/$f" \
      "https://storage.googleapis.com/gvisor/releases/release/$GVISOR_RELEASE/$ARCH/$f"
    curl -fsSL -o "$TMP/$f.sha512" \
      "https://storage.googleapis.com/gvisor/releases/release/$GVISOR_RELEASE/$ARCH/$f.sha512"
  done
  (cd "$TMP" && sha512sum -c runsc.sha512 containerd-shim-runsc-v1.sha512)
  install -m 0755 "$TMP/runsc" "$TMP/containerd-shim-runsc-v1" /usr/local/bin/
  rm -rf "$TMP"

  # Register runsc with the Docker daemon. Merge into daemon.json without
  # clobbering other settings if it already exists.
  install -d -m 0755 /etc/docker
  if [[ -s /etc/docker/daemon.json ]]; then
    jq '.runtimes.runsc = {"path": "/usr/local/bin/runsc"}' /etc/docker/daemon.json \
      > /etc/docker/daemon.json.new && mv /etc/docker/daemon.json.new /etc/docker/daemon.json
  else
    cat > /etc/docker/daemon.json <<'JSON'
{
  "runtimes": {
    "runsc": { "path": "/usr/local/bin/runsc" }
  }
}
JSON
  fi
  systemctl restart docker
fi
echo "[oj-startup] gvisor: $(runsc --version 2>&1 | head -1)"

# ---------- Seccomp profile (Docker hardening variant) ----------------------
install -d -m 0755 /etc/seccomp
echo '${seccomp_profile_b64}' | base64 -d > /etc/seccomp/sandbox-seccomp.json
chmod 0644 /etc/seccomp/sandbox-seccomp.json

# ---------- Drop compose + .env ---------------------------------------------
install -d -m 0755 /opt/oj
echo '${compose_yaml_b64}' | base64 -d > /opt/oj/compute-compose.yml

cat > /opt/oj/.env <<EOF
# Generated by compute.sh on $(date -Iseconds). Tweak terraform vars to change.
AR_URL=${ar_url}
CONTROL_PLANE_IP=${control_plane_ip}
APP_SANDBOX_BACKEND=${sandbox_backend}
APP_SANDBOX_DOCKER_RUNTIME=${sandbox_docker_runtime}
APP_SANDBOX_LINUX_HARDENING_ENABLED=${linux_hardening_enabled}
REGION=${region}
EOF
chmod 0600 /opt/oj/.env

# Pre-pull AR image to avoid a long first-boot stall.
echo "[oj-startup] pre-pulling execution-worker image"
docker pull ${ar_url}/execution-worker:latest || \
  echo "[oj-startup] WARN: pre-pull failed; compose-up will retry"

# ---------- systemd unit ----------------------------------------------------
cat > /etc/systemd/system/oj-compute.service <<'UNIT'
[Unit]
Description=Online Judge — compute (execution-worker) stack
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
EnvironmentFile=/opt/oj/.env
WorkingDirectory=/opt/oj
ExecStart=/usr/bin/docker compose -f /opt/oj/compute-compose.yml up -d --remove-orphans
ExecStop=/usr/bin/docker compose -f /opt/oj/compute-compose.yml down
TimeoutStartSec=600

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable oj-compute.service
systemctl restart oj-compute.service

echo "[oj-startup] $(date -Iseconds) — compute.sh done"
