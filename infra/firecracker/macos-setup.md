# Firecracker on an Apple Silicon Mac — Step-by-step

This is the dev-laptop on-ramp. Goal: get
`FirecrackerExecutionService.execute(...)` running against a **real**
Firecracker microVM in a Linux VM on your Mac, without anything else
breaking.

Constraint: Firecracker needs `/dev/kvm`, which only exists on a Linux host
with hardware virtualization. On Apple Silicon we get there by running an
**ARM** Linux VM via Apple's `Hypervisor.framework` (no kernel extensions,
fast). We use **Lima** to manage the VM; everything is automated by
[`lima.yaml`](./lima.yaml).

---

## Pre-requisites

```sh
brew install lima
```

That's it. Lima brings its own QEMU build for aarch64 + uses
Hypervisor.framework under the hood.

---

## 1. Spin up the VM

From the repo root on your Mac:

```sh
limactl start --name=fc-dev infra/firecracker/lima.yaml
```

First boot pulls the Ubuntu 24.04 ARM cloud image and runs the `provision`
script in `lima.yaml`, which:

1. apt-installs `openjdk-17-jdk-headless` + `curl`,
2. downloads the **aarch64** Firecracker release (v1.10.1) to
   `/usr/local/bin/firecracker`,
3. downloads the Firecracker CI's prebuilt aarch64 kernel + Ubuntu rootfs
   into `/var/lib/firecracker/{vmlinux,rootfs.ext4}` — same paths the
   `app.sandbox.firecracker.*` defaults point at,
4. adds the default user to the `kvm` group so Firecracker can open `/dev/kvm`
   without sudo.

First-time provisioning takes ~3 minutes (mostly the kernel + rootfs
download). Subsequent `limactl start fc-dev` boots in ~5 seconds.

You should see at the end of provisioning:

```
[provision] firecracker: Firecracker v1.10.1
[provision] /dev/kvm:    crw-rw---- 1 root kvm ...
[provision] artifacts:   vmlinux  ~28M, rootfs.ext4  ~400M
```

---

## 2. SSH in and run the microVM smoke test

```sh
limactl shell fc-dev
# you're now inside the Lima VM
/workspace/infra/firecracker/smoke-microvm.sh
```

`smoke-microvm.sh` does the same boot sequence
`FirecrackerExecutionService.execute(...)` does — spawns
`firecracker --api-sock /tmp/fc-smoke-*.sock`, PUTs `/boot-source`,
`/drives/rootfs`, `/machine-config`, `/actions`, then waits for the guest's
kernel/cloud-init banner to appear in the FC log. Expected end-state:

```
[smoke] ✅ microVM booted to userspace. Killing it now.
```

If you see that, the Lima VM is correctly wired and a Firecracker microVM
boots on your Mac. You're done with the infra setup.

---

## 3. Run the execution-worker against Firecracker

Two ways to wire this up. Pick one.

### 3a. Everything-on-Mac, only the worker inside the VM (recommended)

Keep `docker-compose.yml` running on the Mac (Kafka, CRDB, Redis,
Flink, etc. — they don't need KVM and they already work on macOS). Run only
`:execution-worker` inside the Lima VM, pointing back at the Mac for its
infra deps.

On the Mac:
```sh
docker compose up -d kafka zookeeper redis cockroachdb
```

In the Lima VM (`limactl shell fc-dev`):
```sh
cd /workspace
APP_SANDBOX_BACKEND=firecracker \
  APP_SANDBOX_FIRECRACKER_KERNEL_IMAGE=/var/lib/firecracker/vmlinux \
  APP_SANDBOX_FIRECRACKER_ROOTFS_IMAGE=/var/lib/firecracker/rootfs.ext4 \
  SPRING_KAFKA_BOOTSTRAP_SERVERS=host.lima.internal:9092 \
  SPRING_DATASOURCE_URL='jdbc:postgresql://host.lima.internal:26257/defaultdb?sslmode=disable' \
  SPRING_DATA_REDIS_HOST=host.lima.internal \
  ./gradlew :execution-worker:bootRun
```

`host.lima.internal` is the Lima-provided alias for "the Mac". The worker
inside the VM consumes from regional Kafka on the Mac, runs each submission
in a Firecracker microVM inside the Lima VM, publishes verdicts back to
the same Kafka on the Mac. The rest of the pipeline (Flink, leaderboard,
etc.) sees no difference — it's the same `evaluated_results` topic.

Verify via the Mac:
```sh
curl http://localhost:18081/actuator/health   # Lima forwards 8081→18081
```

### 3b. Everything-inside-the-VM

If you want one host with no cross-VM networking, run `docker compose up`
inside the Lima VM too. Lima ships with Docker (or `apt-get install
docker.io` once). Same `docker-compose.yml`, same commands you'd run on
the Mac — just inside the VM. The trade-off is your Mac browser can no
longer hit the services directly without port-forwards.

---

## Day-to-day commands

```sh
# Status / shell
limactl list                              # see VMs and their state
limactl shell fc-dev                      # SSH into the VM
limactl shell fc-dev -- firecracker --version

# Stop / start (preserves disk + provisioning)
limactl stop fc-dev
limactl start fc-dev                      # boots in ~5s after first time

# Nuke and recreate
limactl delete fc-dev
limactl start --name=fc-dev infra/firecracker/lima.yaml

# View provisioning logs
limactl shell fc-dev -- sudo journalctl -u lima-vm-provision -b --no-pager
```

---

## Performance reality check

You're now running on a 3-level virtualization stack:

```
macOS Hypervisor.framework  (level 1: ARM Linux VM, Lima)
   └─ KVM                   (level 2: KVM inside that VM)
        └─ Firecracker VMM  (level 3: per-submission microVM)
```

That's fine for proving the integration is correct. Expect:

- **Boot time per microVM**: ~300–600ms instead of the ~125ms you'd see on
  bare-metal Linux. Most of the overhead is in the L1↔L2 boundary, not
  Firecracker itself.
- **Memory**: each in-flight microVM costs its full `mem_size_mib`
  (256 MiB by default). With 4 GiB allocated to the Lima VM, you'll comfortably
  run a handful of concurrent microVMs but not the production-sized warm
  pool.

Don't benchmark contest throughput from this stack — the numbers will mislead.
But the correctness story (boot → REST → guest exec → host watchdog) is
real, end-to-end, on hardware you own.

---

## Troubleshooting

**`/dev/kvm` is missing inside the VM.**
Make sure you're on Apple Silicon and you used the `aarch64` arch line in
`lima.yaml`. Lima on x86 emulation won't expose KVM. Confirm with
`limactl shell fc-dev -- uname -m` — should print `aarch64`.

**`permission denied` opening `/dev/kvm`.**
The provision script added the login user to the `kvm` group, but group
membership only takes effect on a new login. Either run `limactl stop
fc-dev && limactl start fc-dev` to recycle the session, or `newgrp kvm`
in the current shell.

**Firecracker says `failed to validate vCPU configuration`.**
Almost always means the kernel image arch doesn't match the host arch. Make
sure `/var/lib/firecracker/vmlinux` is an aarch64 kernel — the provision
script downloads the right one, but if you swapped it manually, double-check.

**Microvm boot hangs at `Booting Linux on physical CPU 0x0`.**
The rootfs is corrupt or unreadable. Re-download with:
```sh
sudo curl -fsSL -o /var/lib/firecracker/rootfs.ext4 \
  https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/aarch64/ubuntu-22.04.ext4
```
