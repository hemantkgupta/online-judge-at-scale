# Firecracker on a Raspberry Pi — Step-by-step

Use this when you want **real hardware** running real Firecracker microVMs.
This is the workable path on an Apple Silicon Mac dev setup: your M1/M2 can't
do nested virtualization, but a Pi 4 (8 GB) or Pi 5 sitting on your home LAN
absolutely can.

> **Validated** on a **Raspberry Pi 5 (4 GB)** running **Raspberry Pi OS**
> (Debian Bookworm) with kernel `6.12.62+rpt-rpi-2712`. The full 267-test
> Java suite passes including
> `FirecrackerExecutionServiceTest.execute_bootsRealMicroVMAndCleanlyReturnsAfterTimeout`,
> which drives a real microVM end-to-end via the `FirecrackerExecutionService`
> production code path. The Pi 5's stock kernel ships with KVM built in;
> Pi 4 needs Ubuntu Server 24.04 for KVM support.

Topology:

```
   Mac (M1/M2)                       Raspberry Pi (Pi 5 or Pi 4 8GB)
   ─────────────                     ──────────────────────────────
   docker-compose:                   Ubuntu Server 24.04 ARM64
     • Kafka :9092                   • /dev/kvm available
     • CockroachDB :26257            • Firecracker v1.10.1 installed
     • Redis :6379                   • execution-worker (proto verdicts)
     • Flink, ClickHouse, MM2          consumes from Mac:9092
     • leaderboard-service             runs each submission in a real microVM
                                       publishes verdicts back to Mac:9092
                            ◀── LAN ─▶
```

The worker on the Pi runs the **exact same code path** as on a production
Linux server — `FirecrackerExecutionService` driving the Firecracker REST
socket. The only difference is the box's IP address.

---

## Hardware checklist

| Item | Notes |
|---|---|
| **Pi 5** *(recommended)* or **Pi 4 (8 GB)** | Pi 4 (4 GB) is tight; Pi 3 / Zero won't work — no usable hardware virt |
| **microSD (32 GB+)** or **USB SSD / NVMe HAT** | SSD strongly recommended on Pi 4; SD card I/O is slow and the rootfs gets read on every microVM boot |
| **Cooling** | Active cooler / official Pi 5 cooler / heatsink + fan — sustained load will thermal-throttle without |
| **Wired Ethernet** *(preferred)* or 5 GHz Wi-Fi | Worker ↔ Mac chatter is hot |
| **5 V / 5 A USB-C PSU** | Pi 5 needs the 5 A supply, especially with USB SSD attached |

---

## 1. Flash the Pi

Use **Ubuntu Server 24.04 LTS ARM64**, *not* Raspberry Pi OS. Ubuntu's
`linux-raspi` kernel has KVM enabled by default; Raspberry Pi OS doesn't,
which would mean rebuilding the kernel.

Easiest path:

```sh
brew install raspberry-pi-imager
# Open Raspberry Pi Imager → Choose OS → "Other general-purpose OS"
# → Ubuntu → Ubuntu Server 24.04 LTS 64-bit
# Pre-configure SSH + your wifi/ethernet hostname before flashing.
```

Boot the Pi, find its LAN IP from your router, SSH in. From here everything
runs on the Pi.

---

## 2. Run the setup script

```sh
# On the Pi (SSH'd in)
cd ~
git clone <your-fork-of-online-judge-at-scale> online-judge-at-scale
cd online-judge-at-scale
./infra/firecracker/setup-on-pi.sh
```

The script ([setup-on-pi.sh](./setup-on-pi.sh)) mirrors the Lima
provisioning: installs Firecracker v1.10.1 aarch64, the prebuilt kernel and
rootfs from the Firecracker CI, OpenJDK 17, and adds you to the `kvm` group.
Idempotent — safe to re-run.

Expected end-state:

```
[setup] ✅ Firecracker ready on pi5.local
[setup]   firecracker: Firecracker v1.10.1
[setup]   /dev/kvm:    crw-rw---- 1 root kvm ...
[setup]   artifacts:   vmlinux ~28M, rootfs.ext4 ~400M
```

**Log out and back in** (or `newgrp kvm`) so the new group membership takes
effect.

---

## 3. Smoke-test a real microVM

```sh
~/online-judge-at-scale/infra/firecracker/smoke-microvm.sh
```

Same script that runs inside the Lima VM. On a Pi 5 you should see the
`✅ microVM booted to userspace` line within ~3 seconds. On a Pi 4 it'll be
4–6 seconds.

If you've seen the green check, **a Firecracker microVM is running on your
Raspberry Pi**. The integration is real, not theoretical.

---

## 4. Run the execution-worker on the Pi against the Mac

This is the production-shaped split: keep all the supporting services on
your Mac via `docker-compose`, run only `:execution-worker` on the Pi.

### On the Mac

Bring up the regional Kafka + CRDB + Redis (and anything else you want):

```sh
cd ~/code-all/online-judge-at-scale
docker compose up -d kafka zookeeper redis cockroachdb
```

Find your Mac's LAN IP — the Pi needs to reach it:

```sh
ipconfig getifaddr en0    # Wi-Fi
# or
ipconfig getifaddr en1    # Wired
```

Note that IP (e.g. `192.168.1.42`).

You may also need to allow inbound on the Mac firewall: System Settings →
Network → Firewall → Options → make sure Docker / OrbStack / colima isn't
blocked. On Apple Silicon Macs with default firewall, port 9092 is usually
reachable from the LAN without extra config.

### On the Pi

```sh
cd ~/online-judge-at-scale
APP_SANDBOX_BACKEND=firecracker \
  APP_SANDBOX_FIRECRACKER_KERNEL_IMAGE=/var/lib/firecracker/vmlinux \
  APP_SANDBOX_FIRECRACKER_ROOTFS_IMAGE=/var/lib/firecracker/rootfs.ext4 \
  SPRING_KAFKA_BOOTSTRAP_SERVERS=192.168.1.42:9092 \
  SPRING_DATASOURCE_URL='jdbc:postgresql://192.168.1.42:26257/defaultdb?sslmode=disable' \
  SPRING_DATA_REDIS_HOST=192.168.1.42 \
  ./gradlew :execution-worker:bootRun
```

(Replace `192.168.1.42` with your Mac's actual IP.)

The first build pulls Gradle + Java dependencies — takes a few minutes the
first time, then subsequent runs are seconds.

The worker:

- consumes `submissions.pretest` (proto `SubmissionEvent`) from the Mac's Kafka,
- for each message, spins up a real Firecracker microVM on the Pi,
- runs the submission inside the microVM,
- publishes `VerdictEvent` proto back to the Mac's Kafka,
- gets picked up by Flink (on the Mac), scored, written to Redis, pushed to
  the WebSocket — same pipeline as always.

Verify from the Mac by submitting through the API gateway (or pushing a
proto message directly to `submissions.pretest`) and watching the Pi logs.

---

## Performance & operational notes

- **Boot time per microVM**: ~200 ms (Pi 5) / ~350 ms (Pi 4). Production
  Linux servers see ~125 ms.
- **Throughput**: single Pi 4 sustains ~10–20 microVM-boots/sec before
  thermal throttling kicks in. Pi 5 ~30–50/sec with proper cooling.
- **Memory**: each in-flight microVM gets `app.execution.memory-limit-mb`
  (256 MiB default). 8 GB Pi 4 = ~20 concurrent microVMs with headroom.
- **Storage**: USB SSD on USB 3.0 → rootfs read is ~5× faster than SD card.
  Worth it.
- **Watch `vcgencmd measure_temp`** under sustained load — anything above
  80 °C means you need better cooling.

These numbers are for "I'm proving this works on hardware I own." Don't run
a real contest on a Pi.

---

## Real bugs caught by Pi-side testing

The integration test [`FirecrackerExecutionServiceTest`](../../execution-worker/src/test/java/com/onlinejudge/worker/service/FirecrackerExecutionServiceTest.java)
caught two production-bound bugs when first run on the Pi 5 — neither would
have been visible from the Mac-side unit tests:

1. **Hardcoded `/run` socket directory.** The Java code created the
   Firecracker API socket at `/run/fc-<id>-<uuid>.sock`. `/run` is root-owned
   on every standard Linux distro, so any non-root worker without jailer
   would fail to start Firecracker. Symptom on first Pi run:
   `INTERNAL_ERROR` + `firecracker api socket never appeared`. Fix: the path
   is now configurable via `app.sandbox.firecracker.api-sock-dir` with a
   `/tmp` default that works for any local user.

2. **`init=/init` in default kernel boot args.** Set as a forward-looking
   placeholder for a future custom rootfs with a contestant-code harness
   at `/init`. The Firecracker CI's public Ubuntu rootfs uses
   `/sbin/init` (systemd), not `/init`. Result: kernel can't find the init
   binary, panics, `panic=1 reboot=k` reboots the VM, Firecracker exits 0
   in ~1.5 seconds, and Java records the submission as `OK` with empty
   output — silently 0-exiting every microVM in production. Fix: boot args
   are now configurable via `app.sandbox.firecracker.boot-args`; the default
   omits `init=/init` so any standard rootfs works, and production
   deployments with a custom harness rootfs override.

Both are the kind of bug that mocks don't catch — they only show up when
real `firecracker` and a real KVM kernel meet the orchestration code. The
Pi 5 paid for itself in one afternoon of debugging.

## Troubleshooting

**`/dev/kvm` doesn't appear after `setup-on-pi.sh`.**
Check `lscpu | grep -i virtual` for a "Hypervisor vendor" or VT-related line.
Confirm you're on Ubuntu Server 24.04 (not Raspberry Pi OS) with `cat
/etc/os-release`. If you're on Ubuntu and KVM still won't load, paste
`dmesg | grep -i kvm` — usually the kernel logs the reason.

**Worker on Pi can't connect to Kafka on Mac.**
- Test with `nc -zv 192.168.1.42 9092` from the Pi — should succeed.
- Confirm the Mac's docker-compose exposes Kafka on the host's external
  interface, not just `127.0.0.1`. The repo's `docker-compose.yml` already
  uses `KAFKA_ADVERTISED_LISTENERS` correctly for this.
- Check Mac firewall (System Settings → Network → Firewall).

**Firecracker boot hangs on guest userspace.**
Make sure rootfs.ext4 isn't corrupted from the download. Re-pull with
`curl -fsSL -o /var/lib/firecracker/rootfs.ext4 https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/aarch64/ubuntu-22.04.ext4`.
