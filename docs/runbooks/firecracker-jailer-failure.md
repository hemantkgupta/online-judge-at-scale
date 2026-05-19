# Runbook — Firecracker jailer / chroot failure

> Operator playbook for "every Firecracker boot exits non-zero immediately and the SM cannot replenish the pool". Companion docs: [`../services/sandbox-manager.md#84-firecracker-jailer-chroot-fails`](../services/sandbox-manager.md#84-firecracker-jailer-chroot-fails), [`../tech-spec.md#6-execution-isolation`](../tech-spec.md#6-execution-isolation), [`../design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md), `infra/firecracker/README.md`.

We do not run the `jailer` wrapper today — production today uses the bare `firecracker` binary inside a `privileged: true` container, with cgroups + netns applied host-side by the SM. The naming "jailer failure" is shorthand from the tech-spec runbook table for *"FC's chroot-equivalent isolation broke and the process refuses to start"* — which on our setup surfaces as the FC process immediately exiting 1 (or 126/127), AppArmor / SELinux denials in `dmesg`, or the FC API socket never opening. The hardening path that adds `jailer` proper is tracked in `infra/firecracker/README.md` §Defence in depth.

This runbook covers all three shapes of the same failure: FC won't run at all.

---

## 1. When to run this

- SM logs: `Process exited with code 1` (or `126` / `127`) immediately after a `firecracker` spawn line, repeated for every replenisher tick.
- SM logs: `waitForApiSock timed out` for every provisioning attempt (the FC binary launched but never opened `/tmp/fc-<id>-api.sock`).
- `oj.sandbox.provision.failure_total{reason="firecracker_exit"}` counter incrementing on every tick.
- `dmesg | tail` on the host shows lines like `audit: type=1400 ... apparmor="DENIED" operation="exec"` or `SELinux: avc: denied { execute }`.
- The pool stays at 0 for a language even after a clean SM restart — this distinguishes from [`./warm-pool-empty.md`](./warm-pool-empty.md), where the replenisher works but is just behind. Here it fails on every attempt.

If the FC process boots and prints its kernel banner but the agent never reports ready, the runbook is [`./warm-pool-empty.md#44-agent-readiness-timeout`](./warm-pool-empty.md#44-agent-readiness-timeout) — that's a guest-side issue, not a host-side isolation issue.

---

## 2. Detection

### 2.1 Look at the SM's last attempt

```sh
sudo docker logs oj-sandbox-manager --tail 400 2>&1 \
  | grep -E 'firecracker|FirecrackerLauncher|Process exited|waitForApiSock' \
  | tail -40
```

Most informative signature is the **exit code** plus the **argv** the SM logged. Sample bad lines:

```
[fc-launcher] spawn argv=[ip, netns, exec, oj-sb-12ab, firecracker, --api-sock, /tmp/fc-...-api.sock]
[fc-launcher] Process exited with code 1 (sandbox=sb-12ab, language=python)
[pool] POOL provision failed: firecracker exit (sandbox=sb-12ab, reason=firecracker_exit)
```

Exit-code map (LinuxProcess + Firecracker conventions):

| Code | Most likely cause |
|---|---|
| 1   | FC saw a config error (machine-config, drive path, network) — runtime guest-config issue |
| 2   | FC argv parse error — we changed argv and it's malformed |
| 126 | Cannot execute (file exists but not executable) — permissions / `noexec` mount |
| 127 | Binary not found at the path SM expects (`app.firecracker.binary`) |
| 137 | OOM-kill by host kernel — `dmesg` confirms |
| 139 | SEGV — FC crash; likely incompatible kernel/rootfs |

### 2.2 Verify the host-side primitives

```sh
# Run the FC binary by hand to see the actual error
sudo docker exec oj-sandbox-manager firecracker --version
# Should print 'Firecracker v1.x.x'. Exits != 0 → binary missing or unrunnable.

# /dev/kvm reachable from inside the SM container?
sudo docker exec oj-sandbox-manager ls -la /dev/kvm
# crw-rw---- 1 root kvm ... = good
# 'No such file or directory' = compose missing the device mount

# Is the netns prefix even reachable?
sudo docker exec oj-sandbox-manager ip netns list
```

### 2.3 Check kernel audit log

```sh
sudo dmesg --since '10 minutes ago' | grep -iE 'apparmor|selinux|denied|firecracker|kvm' | tail -30
```

A modern Ubuntu host with Docker installed via the official packages ships an AppArmor profile for `docker-default`. If the GCE image was updated mid-deploy (e.g. `ubuntu-2204-lts` → `ubuntu-2404-lts`), the new image's profile may be stricter than the old one's.

---

## 3. Diagnose

### 3.1 Argv shape

The SM builds the argv in `FirecrackerLauncher.buildFirecrackerArgv()`:

- Egress lockdown ON (production default): `["ip", "netns", "exec", netnsName, "firecracker", "--api-sock", apiSock]`
- Egress lockdown OFF (CI / macOS): `["firecracker", "--api-sock", apiSock]`

If the `ip netns exec` prefix path is the failing one, run it manually:

```sh
sudo docker exec -it oj-sandbox-manager sh -c '
  ip netns add oj-debug-ns &&
  ip netns exec oj-debug-ns firecracker --version &&
  ip netns del oj-debug-ns
'
```

If this fails before reaching `firecracker --version`, the netns layer is broken — usually `iproute2` not installed (rare on production GCE; common on hand-built dev hosts) or kernel `CONFIG_NET_NS=n` (impossible on stock GCE).

If `firecracker --version` itself fails inside the netns, drop the netns prefix temporarily to confirm:

```sh
# Edit /opt/oj/region.yml under oj-sandbox-manager.environment:
#   APP_SANDBOX_EGRESS_LOCKDOWN_ENABLED=false
sudo docker compose -f /opt/oj/region.yml up -d oj-sandbox-manager
```

This will let the pool fill but **disables the egress lockdown** — temporary diagnostic only, not a permanent fix.

### 3.2 AppArmor / SELinux denial

```sh
# Confirm AppArmor is enforcing
sudo aa-status 2>/dev/null | head

# Look at the actual denial
sudo dmesg --since '10 minutes ago' | grep DENIED | tail
```

A line like:

```
audit: type=1400 audit(...): apparmor="DENIED" operation="exec" profile="docker-default" name="/usr/local/bin/firecracker" ...
```

means the Docker AppArmor profile is blocking FC. The fix is to launch the SM container with an unconfined or custom profile (see §4.2).

### 3.3 Rootfs / kernel mismatch

If FC starts (no SM exit log) but the API socket never opens (`waitForApiSock timed out`), the guest kernel boots and crashes before mounting the rootfs:

```sh
# Reproduce manually with the API enabled
sudo docker exec oj-sandbox-manager firecracker --api-sock /tmp/fc-debug.sock &
# In another shell, drive the API to load the vmlinux + rootfs:
sudo docker exec oj-sandbox-manager sh -c '
  curl --unix-socket /tmp/fc-debug.sock -i \
    -X PUT -H "Content-Type: application/json" \
    -d "{\"kernel_image_path\":\"/var/lib/firecracker/vmlinux\",\"boot_args\":\"console=ttyS0 reboot=k panic=1 pci=off\"}" \
    http://localhost/boot-source
'
```

A 400 from the FC API call means the kernel image is corrupt or the path is wrong. A 200 followed by silence means the kernel boots but panics — capture the serial console by adding `console=ttyS0` and reading FC's stdout (which the SM redirects via `redirectErrorStream(true)`).

---

## 4. Mitigate

### 4.1 Binary missing or wrong path (exit 127)

```sh
ls -la /var/lib/firecracker/    # vmlinux + rootfs.ext4 expected
which firecracker || find / -name firecracker -type f 2>/dev/null | head

# If binary is missing, re-fetch via the harness build
sudo bash /opt/oj/build-rootfs.sh
# Then restart SM
sudo docker compose -f /opt/oj/region.yml restart oj-sandbox-manager
```

`build-rootfs.sh` (from `infra/firecracker/rootfs/build-rootfs.sh`) downloads Firecracker v1.x and writes it to `/usr/local/bin/firecracker` on the host. The SM container bind-mounts this path.

### 4.2 AppArmor / SELinux denial

Per [`../services/sandbox-manager.md#84-firecracker-jailer-chroot-fails`](../services/sandbox-manager.md#84-firecracker-jailer-chroot-fails), the fix is to give the SM container a less-restrictive security profile.

Quick unblock (good enough for dev / non-prod):

```sh
# Edit /opt/oj/region.yml under oj-sandbox-manager:
#   security_opt:
#     - seccomp=unconfined
#     - apparmor=unconfined
sudo docker compose -f /opt/oj/region.yml up -d oj-sandbox-manager
```

Production posture: write a custom AppArmor profile that allows FC's `mmap` + `kvm` syscalls and the netns prefix. Today the project does not ship a custom profile — see `infra/firecracker/README.md` §Defence in depth for the path forward (the full `jailer` wrapper subsumes this concern).

### 4.3 Rootfs rebuild

If the kernel boots but panics on rootfs mount (4.3 from §3 above), rebuild:

```sh
sudo bash /opt/oj/build-rootfs.sh
ls -la /var/lib/firecracker/rootfs.ext4   # mtime should be just now

# Bounce SM to drop any half-provisioned sandboxes
sudo docker compose -f /opt/oj/region.yml restart oj-sandbox-manager
```

If `build-rootfs.sh` itself fails, it almost always means `debootstrap` couldn't reach the Ubuntu mirror — check `/etc/resolv.conf` and re-run.

### 4.4 cgroup / netns failure (exit 1 with cgroup error)

```sh
# Confirm the parent cgroups exist
ls -la /sys/fs/cgroup/memory/oj/ /sys/fs/cgroup/cpu/oj/ 2>/dev/null
# If missing, the SM should create them on boot; bouncing fixes:
sudo docker compose -f /opt/oj/region.yml restart oj-sandbox-manager
```

Per [`../services/sandbox-manager.md#36-netnsapplier`](../services/sandbox-manager.md#36-netnsapplier), the SM expects `/sys/fs/cgroup` mounted into the container. The compose entry must have `volumes: [/sys/fs/cgroup:/sys/fs/cgroup:rw]`.

### 4.5 Once FC is healthy, verify

```sh
curl -sX POST http://localhost:9100/lease \
  -H 'Content-Type: application/json' \
  -d '{"language":"python","submission_id":"diag-post-fix","time_limit_ms":1000}' | jq

# Then release the diag sandbox
curl -sX POST http://localhost:9100/release \
  -H 'Content-Type: application/json' \
  -d '{"sandbox_id":"<from above>"}'
```

The 200 response shape includes `vsock_uds_path`. After this works, the pool should fill on its own.

---

## 5. Rollback / if mitigation went wrong

### 5.1 Disabled egress lockdown for diagnostics — re-enable

If §3.1 set `APP_SANDBOX_EGRESS_LOCKDOWN_ENABLED=false`, **re-enable it immediately** once FC is launching cleanly. The egress lockdown is the seal that keeps submission code from reaching the internet (and exfiltrating problem-service signed URLs, or DDoS'ing anyone). Running production without it is a security incident waiting to happen — see [`../design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md).

```sh
# Restore production default
sudo sed -i 's/APP_SANDBOX_EGRESS_LOCKDOWN_ENABLED=false/APP_SANDBOX_EGRESS_LOCKDOWN_ENABLED=true/' /opt/oj/region.yml
sudo docker compose -f /opt/oj/region.yml up -d oj-sandbox-manager
```

### 5.2 Loosened security profile broke unrelated things

If you set `seccomp=unconfined` + `apparmor=unconfined` to unblock FC and now other containers on the same VM behave oddly, the security policy change leaked. The SM is the only container that needs this carve-out — never apply it cluster-wide. Revert any blanket relaxation; keep it scoped to `oj-sandbox-manager`.

### 5.3 Rebuilt rootfs is broken

If the new rootfs boots FC but the in-guest agent never reports ready (`waitForApiSock` passes but readiness times out), the rebuild dropped the agent binary. Fix path:

```sh
# Check what the rootfs actually contains
sudo mkdir -p /mnt/rootfs-debug
sudo mount -o loop /var/lib/firecracker/rootfs.ext4 /mnt/rootfs-debug
ls /mnt/rootfs-debug/usr/local/bin/   # agent + oj-vsock-client expected
sudo umount /mnt/rootfs-debug
```

If `agent` is missing, the rootfs was rebuilt before the Go agent was rebuilt. Re-run `infra/firecracker/agent/build.sh` (or whatever the harness does for that step — see `infra/firecracker/README.md`).

### 5.4 Disabled iptables and now the egress test fails

`app.sandbox.egress-lockdown.iptables-enabled=true` is the production default on GCE hosts and `false` on macOS / CI. If you swapped this off as a diagnostic, restore it before the next release. The Go integration test at `infra/firecracker/agent/cmd/agent/egress_test.go` (build tag `integration_microvm`) is the canary — run it in a sandbox to confirm lockdown is back.

---

## 6. Related incidents

- [`./warm-pool-empty.md`](./warm-pool-empty.md) — the upstream observable. A FC-cannot-boot situation manifests as a permanently-empty pool. Always read both.
- [`../services/sandbox-manager.md#85-iptables-rules-accumulating`](../services/sandbox-manager.md#85-iptables-rules-accumulating) — a related cleanup pathology if SM has been crash-looping; orphan iptables rules from dead sandboxes pile up.
- [`../services/sandbox-manager.md#86-devkvm-permission-denied`](../services/sandbox-manager.md#86-devkvm-permission-denied) — the narrow `/dev/kvm` permission case, a subset of §4.1 here.
- [`../design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md) — why the netns + iptables prefix exists; mandatory reading before relaxing the egress lockdown for "just a diagnostic".

---

## 7. Escalation

- AppArmor / SELinux denial on production GCE images is a one-shot fix (custom profile) but it requires either an image swap or a host-side profile install. Escalate to the platform owner; do not bake the workaround into `region.yml` permanently.
- Repeated exit-127 (binary missing) on a fresh VM means the startup script failed to install Firecracker. Pull the serial-console output of the most recent boot:
  ```sh
  gcloud compute instances get-serial-port-output oj-compute --zone=asia-south1-a | tail -300
  ```
- If FC works but pool fills 0 within 10 s of refill (sandboxes booting and immediately dying), the rootfs is broken in a way that boots fine but crashes after the kernel hands control. Snapshot the FC stdout (SM redirects to its own logs) for the panic line and escalate to whoever owns `infra/firecracker/`.
- Never leave egress lockdown disabled overnight. If you cannot solve the FC issue with lockdown enabled within a few hours, stop the compute VM rather than running unconfined.
