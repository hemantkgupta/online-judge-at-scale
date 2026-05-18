#!/usr/bin/env bash
# =============================================================================
# Region VM startup script (terraform templatefile target).
#
# Runs on every boot of EITHER region VM via VM metadata `startup-script`.
# Each VM runs the FULL application stack — control plane services (Kafka,
# CRDB, Redis, api-gateway, problem-service, contest-service, leaderboard-
# service) plus compute services (sandbox-manager, execution-worker with
# Firecracker / Docker / gVisor backends).
#
# Idempotent: each step is a no-op if already done. Safe to re-run on boot.
#
# Template variables interpolated by terraform:
#   ${ar_url}                  e.g. asia-south1-docker.pkg.dev/online-judge-hk/oj-images
#   ${region}                  the region THIS VM is in (e.g. asia-south1)
#   ${zone}                    the zone THIS VM is in
#   ${peer_region}             the other region (e.g. us-central1)
#   ${peer_internal_ip}        the other VM's internal IP — CRDB --join + cross-region traffic
#   ${jwt_secret}              48-byte random secret (terraform random_password)
#   ${compose_yaml_b64}        base64-encoded region.yml
#   ${gcs_bucket_name}         GCS bucket holding per-ordinal input/expected objects
#   ${gcs_signer_secret}       Secret Manager secret id holding the signer SA JSON key
#   ${otel_config_yaml_b64}    base64-encoded otel-collector-config.yaml
#   ${seccomp_profile_b64}     base64-encoded sandbox-seccomp.json
#   ${rootfs_init_b64}         base64-encoded Firecracker rootfs init.sh
#   ${rootfs_builder_b64}      base64-encoded build-rootfs.sh
#   ${agent_src_zip_b64}       base64-encoded zip of Execution Agent source
#   ${sandbox_backend}         docker | firecracker
#   ${sandbox_docker_runtime}  runc | runsc
#   ${linux_hardening_enabled} true | false
#   ${project_id}              GCP project ID (for GCS_PROJECT_ID env)
#
# All other config (kafka host IP, AR URL, JWT secret, peer-IP wiring) lands
# in /opt/oj/.env which the systemd unit sources via `EnvironmentFile=`.
# =============================================================================

set -euo pipefail

LOG=/var/log/oj-startup.log
exec > >(tee -a "$LOG") 2>&1
echo "[oj-startup] $(date -Iseconds) — region.sh begin (region=${region}, peer=${peer_region})"

# Pinned versions. Bump deliberately, not by accident.
FIRECRACKER_VERSION="v1.10.1"
FIRECRACKER_CI_BASE="https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/x86_64"
GVISOR_RELEASE="20240826"

# ---------- Docker install (idempotent) -------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  echo "[oj-startup] installing docker engine + compose plugin"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg lsb-release jq
  install -m 0755 -d /etc/apt/keyrings
  # --batch --no-tty: gpg 2.x in systemd-run startup scripts has no
  # controlling terminal; without these flags it tries to open /dev/tty.
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    gpg --batch --no-tty --yes --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
  usermod -aG docker hemant || true
else
  echo "[oj-startup] docker already installed: $(docker --version)"
fi

# ---------- gcloud (for AR auth + Secret Manager fetch) ---------------------
if ! command -v gcloud >/dev/null 2>&1; then
  echo "[oj-startup] installing google-cloud-cli"
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --batch --no-tty --yes --dearmor -o /usr/share/keyrings/cloud.google.gpg
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list
  apt-get update -y
  apt-get install -y google-cloud-cli
fi

# ---------- Artifact Registry auth ------------------------------------------
AR_REGISTRY="$(echo '${ar_url}' | cut -d/ -f1)"
echo "[oj-startup] configuring docker auth for $AR_REGISTRY"
gcloud auth configure-docker "$AR_REGISTRY" --quiet

# ---------- Firecracker binary + kernel -------------------------------------
# Both VMs run Firecracker — /dev/kvm must exist (nested-virt enabled in
# terraform). Add `hemant` and `docker` users to kvm so privileged containers
# can open it.
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

# Guest kernel from the Firecracker CI bucket.
if [[ ! -s /var/lib/firecracker/vmlinux ]]; then
  echo "[oj-startup] downloading guest kernel (vmlinux-5.10.223)"
  curl -fsSL -o /var/lib/firecracker/vmlinux \
    "$FIRECRACKER_CI_BASE/vmlinux-5.10.223"
fi

# ---------- Custom OJ harness rootfs ----------------------------------------
echo "[oj-startup] installing rootfs build deps (debootstrap, e2fsprogs, unzip)"
apt-get install -y --no-install-recommends debootstrap e2fsprogs unzip

install -d -m 0755 /opt/oj/rootfs-builder
echo '${rootfs_init_b64}'    | base64 -d > /opt/oj/rootfs-builder/init.sh
echo '${rootfs_builder_b64}' | base64 -d > /opt/oj/rootfs-builder/build-rootfs.sh
chmod +x /opt/oj/rootfs-builder/init.sh /opt/oj/rootfs-builder/build-rootfs.sh

echo "[oj-startup] staging Execution Agent source for build-rootfs"
install -d -m 0755 /opt/oj/agent
echo '${agent_src_zip_b64}' | base64 -d > /tmp/oj-agent-src.zip
unzip -q -o /tmp/oj-agent-src.zip -d /opt/oj/agent
rm -f /tmp/oj-agent-src.zip

# build-rootfs.sh self-skips if the existing rootfs has the correct version
# marker, so re-running this script on subsequent boots is a no-op once the
# rootfs is built.
echo "[oj-startup] building harness rootfs (first time ~10 min, then cached)"
AGENT_SRC_DIR=/opt/oj/agent /opt/oj/rootfs-builder/build-rootfs.sh

# ---------- gVisor (runsc) registered as a Docker runtime -------------------
# Non-fatal — keep the install step but treat any failure as a warning, so a
# future URL change or transient 404 doesn't block the entire VM boot.
install_gvisor() {
  if command -v runsc >/dev/null 2>&1; then
    return 0
  fi
  echo "[oj-startup] installing gvisor ($GVISOR_RELEASE)"
  local ARCH TMP
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
}

if ! install_gvisor; then
  echo "[oj-startup] WARN: gvisor install failed; runsc runtime unavailable on this host" >&2
fi

if command -v runsc >/dev/null 2>&1; then
  echo "[oj-startup] gvisor: $(runsc --version 2>&1 | head -1)"
else
  echo "[oj-startup] gvisor: NOT INSTALLED (non-fatal, see WARN above)"
fi

# ---------- Seccomp profile (Docker hardening variant) ----------------------
install -d -m 0755 /etc/seccomp
echo '${seccomp_profile_b64}' | base64 -d > /etc/seccomp/sandbox-seccomp.json
chmod 0644 /etc/seccomp/sandbox-seccomp.json

# ---------- Drop compose + collector config ---------------------------------
install -d -m 0755 /opt/oj
echo '${compose_yaml_b64}'     | base64 -d > /opt/oj/region.yml
echo '${otel_config_yaml_b64}' | base64 -d > /opt/oj/otel-collector-config.yaml

# Internal IP of this VM, discovered from the GCE metadata service. Used as
# the advertised Kafka host AND the CRDB --advertise-addr.
INTERNAL_IP="$(curl -sfH 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)"

cat > /opt/oj/.env <<EOF
# Generated by region.sh on $(date -Iseconds). Do not edit by hand —
# changes are clobbered on next boot. Tweak terraform vars instead.
AR_URL=${ar_url}
REGION=${region}
PEER_REGION=${peer_region}
PEER_INTERNAL_IP=${peer_internal_ip}
INTERNAL_IP=$INTERNAL_IP
KAFKA_HOST_EXTERNAL=$INTERNAL_IP
JWT_SECRET=${jwt_secret}
GCS_BUCKET=${gcs_bucket_name}
PROJECT_ID=${project_id}
OTEL_JAVAAGENT_ENABLED=false
OTEL_ENDPOINT=http://oj-otel-collector:4317
APP_SANDBOX_BACKEND=${sandbox_backend}
APP_SANDBOX_DOCKER_RUNTIME=${sandbox_docker_runtime}
APP_SANDBOX_LINUX_HARDENING_ENABLED=${linux_hardening_enabled}
APP_PROBLEM_SERVICE_REQUIRED=true
APP_PROBLEM_SERVICE_URL=http://oj-problem-service:8089
EOF
chmod 0600 /opt/oj/.env

# ---------- Fetch problem-service signer key from Secret Manager ------------
echo "[oj-startup] fetching problem-service signer key from Secret Manager"
if gcloud secrets versions access latest --secret="${gcs_signer_secret}" \
     > /opt/oj/gcs-signer.json.new 2>/dev/null; then
  chmod 0400 /opt/oj/gcs-signer.json.new
  mv /opt/oj/gcs-signer.json.new /opt/oj/gcs-signer.json
  echo "[oj-startup] gcs-signer.json ready"
else
  echo "[oj-startup] WARN: secret ${gcs_signer_secret} not accessible — problem-service will fail GCS signing"
  rm -f /opt/oj/gcs-signer.json.new
fi

# ---------- Pre-pull AR images (avoid first-boot stall) ---------------------
for IMG in api-gateway problem-service contest-service leaderboard-service sandbox-manager execution-worker; do
  echo "[oj-startup] pre-pulling $IMG"
  docker pull ${ar_url}/$IMG:latest || \
    echo "[oj-startup] WARN: pre-pull of $IMG failed; compose-up will retry"
done

# ---------- systemd unit ----------------------------------------------------
cat > /etc/systemd/system/oj-region.service <<'UNIT'
[Unit]
Description=Online Judge — region docker-compose stack (full app stack)
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
EnvironmentFile=/opt/oj/.env
WorkingDirectory=/opt/oj
ExecStart=/usr/bin/docker compose -f /opt/oj/region.yml up -d --remove-orphans
ExecStop=/usr/bin/docker compose -f /opt/oj/region.yml down
TimeoutStartSec=900

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable oj-region.service
systemctl restart oj-region.service

echo "[oj-startup] $(date -Iseconds) — region.sh done"
