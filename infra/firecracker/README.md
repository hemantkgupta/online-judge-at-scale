# Firecracker MicroVM Sandbox — Operator Notes

This directory documents how to bring the `FirecrackerExecutionService`
backend live. The Java host-side orchestration lives in
[`execution-worker/.../FirecrackerExecutionService.java`](../../execution-worker/src/main/java/com/onlinejudge/worker/service/FirecrackerExecutionService.java);
the items below are the operator-side artifacts the application expects to
find on disk and the kernel features it depends on.

> **Quick dev paths for hands-on testing:**
>
> * **Apple Silicon Mac (M3+, macOS 15+):** [`macos-setup.md`](./macos-setup.md).
>   Lima VM with nested-virt → real microVM on your laptop in three commands.
> * **Apple Silicon Mac (M1 / M2):** No nested virt support in the silicon.
>   Use the Raspberry Pi path below instead.
> * **Raspberry Pi 4 (8 GB) or Pi 5:** [`raspberry-pi-setup.md`](./raspberry-pi-setup.md).
>   Real hardware, real KVM. Worker on the Pi consumes from Kafka running on
>   your Mac via the LAN. Setup script: [`setup-on-pi.sh`](./setup-on-pi.sh).
>
> Both paths drop you at the same end state: `smoke-microvm.sh` prints
> `✅ microVM booted to userspace`. From there the worker runs unchanged.

## Why Firecracker, not Docker

Docker gives you a *shared-kernel* sandbox: every container on a host runs
against the same Linux kernel. A kernel CVE in any one container's syscall
path is a host-wide escalation primitive. For trusted local development that's
fine; for code submitted by anonymous contestants on an internet-facing
judge it isn't.

Firecracker gives you a *hardware-isolated* sandbox: each microVM has its
own guest kernel running under KVM. A successful kernel escape inside the
microVM hits a virtualised hardware boundary, not the host kernel. Same
security model AWS Lambda and Fargate use. Apache 2.0, no vendor lock-in.

## Prerequisites

- **Linux host** with `/dev/kvm` exposed (i.e. nested virtualisation enabled
  on the cloud VM, or a bare-metal box).
- The runtime user is in the `kvm` group (`getfacl /dev/kvm` should show the
  worker uid having `rw`).
- `curl` available on `$PATH` — the Java host uses it to drive the
  Firecracker REST socket. Replace with a native AF_UNIX HTTP client in
  production if you want one less subprocess hop.
- Linux kernel ≥ 4.14 with KVM, virtio, vsock modules.

## Files the worker expects

| Path | What it is | Default config key |
|---|---|---|
| `/usr/local/bin/firecracker` | The Firecracker binary | `app.sandbox.firecracker.binary` |
| `/var/lib/firecracker/vmlinux` | Stripped Linux kernel image (no modules) | `app.sandbox.firecracker.kernel-image` |
| `/var/lib/firecracker/rootfs.ext4` | ext4 rootfs containing the language runtimes + an `/init` that runs the submitted code | `app.sandbox.firecracker.rootfs-image` |

Override any of those via the matching `app.sandbox.firecracker.*` property
or environment variable.

## Building the kernel and rootfs

The Firecracker team publishes pre-built artifacts; for production you want
to build your own so the surface is minimal.

```sh
# 1. Kernel — strip to bare essentials. The official Firecracker repo includes
#    `microvm-kernel-config` recipes per arch.
git clone https://github.com/firecracker-microvm/firecracker.git
cd firecracker/resources
make microvm-kernel-x86_64

# 2. Rootfs — Debian base + python3 + openjdk + gcc + an /init that:
#       - mounts the code drive,
#       - runs `solution.<ext>` inside a cgroup,
#       - writes stdout to a shared file,
#       - calls `reboot` to terminate the VM.
debootstrap --variant=minbase bookworm rootfs http://deb.debian.org/debian
chroot rootfs apt-get install -y python3 openjdk-17-jdk-headless g++
mkfs.ext4 -d rootfs rootfs.ext4 512M

# 3. Drop both artifacts into /var/lib/firecracker/.
install -m 0644 firecracker/build/linux/.../vmlinux  /var/lib/firecracker/vmlinux
install -m 0644 rootfs.ext4                          /var/lib/firecracker/rootfs.ext4
```

## Switching the worker to Firecracker

```yaml
# execution-worker/src/main/resources/application.yml (or env override)
app:
  sandbox:
    backend: firecracker
    firecracker:
      vcpus: 1
```

Or via env var:

```sh
APP_SANDBOX_BACKEND=firecracker ./gradlew :execution-worker:bootRun
```

On a non-Linux host the application **refuses to start** with a clear error
message — misconfiguration fails loudly at boot rather than at the first
submission. The Docker backend remains the default and works on macOS.

## What the worker drives at runtime

For each submission the Java host:

1. Creates a per-submission Unix socket at `/run/fc-<submissionId>-<uuid>.sock`.
2. Spawns `firecracker --api-sock <sock>`.
3. PUTs `/boot-source`, `/drives/rootfs`, `/machine-config` to the socket.
4. PUTs `/actions {"action_type":"InstanceStart"}` — the VM boots, `/init`
   compiles + runs the code, writes stdout to a shared file.
5. Waits for the firecracker process to exit (or wall-clock timeout —
   `app.execution.timeout-seconds`, default 5s).
6. Reads stdout, kills the VM if still alive, deletes the socket and
   per-submission tempdir.

The end-of-execution kill is destroy-never-reuse — the same lifecycle the
Docker backend uses with `--rm`. No state survives between submissions.

## Defence in depth — jailer

Production Firecracker deployments wrap the binary in [`jailer`](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md),
which adds chroot, setuid/setgid, cgroups, and pivot_root before launching
the VMM. If you ship this, set `app.sandbox.firecracker.binary` to the
`jailer` wrapper script rather than `firecracker` directly. The host-side
Java orchestration doesn't need to know — `jailer` execs `firecracker` after
the namespace dance, and the REST socket comes up the same way.
