# Runbook — Warm pool empty > 30s

> Operator playbook for "the sandbox-manager has no warm Firecracker VMs for one or more languages, and the worker is nack'ing submissions in a loop". Companion docs: [`../services/sandbox-manager.md`](../services/sandbox-manager.md), [`../services/execution-worker.md#85-pool-exhausted-retry-worker`](../services/execution-worker.md), [`../tech-spec.md#85-pool-exhausted-retry-worker`](../tech-spec.md#85-pool-exhausted-retry-worker), [`../tech-spec.md#61-pool-state-machine`](../tech-spec.md#61-pool-state-machine).

The warm pool is the buffer between Kafka-driven submission arrivals and the ~150 ms Firecracker boot cost. When it empties, the SM returns `503 pool_exhausted` and the worker re-queues messages via `ack.nack(retry_after_ms)`. The contestant sees PENDING longer than usual; if the pool stays empty for over 30 s the alert fires.

---

## 1. When to run this

- Alert: `oj.sandbox.pool.ready{language=X} == 0` for ≥ 30 s.
- Worker logs show repeated `PoolExhaustedException` / `[firecracker] sandbox-manager call failed ... HTTP 503 pool_exhausted` for the same language.
- `oj.sandbox.pool.exhausted_total{language=X}` counter is incrementing every few seconds.
- `oj.worker.submission.duration_seconds` p99 climbs sharply (each submission is paying the full nack-retry delay).
- Sustained — not a one-off spike. A single 503 with `retry_after_ms=250` is the design (see tech-spec §8.5); a *minute-long* drought is the problem.

If you see this **only on startup** of `oj-compute`, the SM is still mid-replenish — wait 60 s. If it persists, continue.

---

## 2. Detection

### 2.1 Confirm the pool is actually empty

```sh
# On oj-compute
sudo docker exec oj-sandbox-manager sh -c 'curl -s localhost:9100/actuator/health' | jq

# The pool-ready gauge per language (requires OTel collector to be live;
# falls back to the actuator metrics endpoint)
sudo docker exec oj-sandbox-manager sh -c \
  'curl -s localhost:9100/actuator/metrics/oj.sandbox.pool.ready' 2>/dev/null | jq

# Direct: try to lease one
curl -sX POST http://localhost:9100/lease \
  -H 'Content-Type: application/json' \
  -d '{"language":"python","submission_id":"diag-1","time_limit_ms":1000}' | jq
# 200 → pool is fine, alert is stale
# 503 with {"error":"pool_exhausted","retry_after_ms":250} → confirmed empty
# 500 → KVM/firecracker problem, jump to docs/runbooks/firecracker-jailer-failure.md
```

### 2.2 Verify which language

```sh
# Counts how many provisions failed in the last 200 log lines, by language
sudo docker logs oj-sandbox-manager --tail 200 2>&1 \
  | grep -E 'POOL provision failed|PoolExhausted' \
  | grep -oE 'language=[a-z+]+' | sort | uniq -c
```

The pool is per-language. Python may be empty while C++ and Java are fine, or vice versa.

---

## 3. Diagnose

### 3.1 Read the SM logs first

```sh
sudo docker logs oj-sandbox-manager --tail 400 2>&1 \
  | grep -E 'POOL provision failed|Failed to provision|PoolExhausted|firecracker|KVM' \
  | tail -60
```

The most common signatures and what they mean:

| Log line | Cause | Section |
|---|---|---|
| `POOL provision failed: KVM not available` | `/dev/kvm` missing or not mounted | §4.1 |
| `POOL provision failed: firecracker binary not found` | `app.firecracker.binary` path wrong, or the rootfs build never ran | §4.2 |
| `POOL provision failed: rootfs not found` | `/var/lib/firecracker/rootfs.ext4` missing on the host | §4.2 |
| `Process exited with code 1` immediately after spawning FC | FC argv shape changed, OR jailer/AppArmor denial | [`./firecracker-jailer-failure.md`](./firecracker-jailer-failure.md) |
| `waitForApiSock timed out` after 30 s | FC booted but never opened the API socket — usually KVM saturation | §4.3 |
| `agent readiness timeout` | FC boots, agent never prints its readiness line. Rootfs / kernel mismatch. | §4.4 |
| (no errors, but `pool.ready` still 0) | Replenisher thread alive but not catching up to demand | §4.5 |

### 3.2 KVM and host capacity

```sh
# Is /dev/kvm actually exposed?
sudo docker exec oj-sandbox-manager ls -la /dev/kvm

# How many FC processes are running?
sudo docker exec oj-sandbox-manager sh -c 'ps -eo pid,cmd | grep -c "[f]irecracker"'

# Host-level CPU pressure (FC boot pegs one vCPU per child)
top -bn1 | head -5

# Host-level memory
free -m
```

If `ps` shows zero FC processes AND the container is up AND `app.pool.targets.python ≥ 1`, the replenisher is not running — bounce SM (§4.5).

### 3.3 SPOT preemption side effect

`oj-compute` is a SPOT VM. Google can reclaim it with 30 s notice. After preemption + auto-restart, the SM container boots and the replenisher takes 60–120 s to fill the pool. The first ~30 s of that window is normal-looking "pool empty" — alerts on a 30 s window will fire spuriously.

```sh
# Recent preemption?
gcloud compute operations list \
  --filter='operationType=compute.instances.preempted AND targetLink~oj-compute' \
  --limit=5
```

If preemption is within the last 2 minutes, **wait** — don't restart, you'll just delay the recovery.

---

## 4. Mitigate

### 4.1 KVM missing

```sh
# Confirm the compose has /dev/kvm
grep -A2 'oj-sandbox-manager' /opt/oj/region.yml | grep -E 'privileged|devices'
# Expected:
#   privileged: true
#   devices: ["/dev/kvm:/dev/kvm"]
```

If the entries are absent, this VM is misconfigured — `region.yml` should match the repo template. Reconcile:

```sh
sudo docker compose -f /opt/oj/region.yml stop oj-sandbox-manager
# Edit /opt/oj/region.yml to add the privileged + devices lines under oj-sandbox-manager
sudo docker compose -f /opt/oj/region.yml up -d oj-sandbox-manager
```

If `/dev/kvm` simply doesn't exist on the host, the GCE VM is missing nested virtualization. Verify with `ls -la /dev/kvm`. Terraform should have set `enable_nested_virtualization = true` on `oj-compute`. If it didn't, the only fix is to recreate the instance (the flag is set-once on creation).

### 4.2 Rootfs / FC binary missing

```sh
# Are the artifacts where SM expects them?
ls -la /var/lib/firecracker/

# If empty/missing, rebuild on the host (root-only)
sudo bash /opt/oj/build-rootfs.sh        # writes vmlinux + rootfs.ext4
sudo docker compose -f /opt/oj/region.yml restart oj-sandbox-manager
```

`build-rootfs.sh` ships from `infra/firecracker/rootfs/build-rootfs.sh` via the startup script. Takes ~3 minutes — it `debootstrap`s a tiny Ubuntu root and installs `python3 / openjdk-17-jdk-headless / g++` (see `infra/firecracker/README.md`). Re-run is idempotent.

### 4.3 KVM saturation (post-preemption or load spike)

When the host has just rebooted and the replenisher is trying to boot N VMs in parallel, the cap `app.pool.max-parallel-boot=2` keeps it from thrashing — but recovery still takes ~60 s on a 2-vCPU host. Lean on the cap; don't bump it without thinking:

```sh
# Check current cap
sudo docker exec oj-sandbox-manager sh -c 'env | grep APP_POOL_MAX_PARALLEL_BOOT'

# Look at provision durations
sudo docker exec oj-sandbox-manager sh -c \
  'curl -s localhost:9100/actuator/metrics/oj.sandbox.provision.duration_seconds' | jq
```

Healthy: < 500 ms for python/cpp, < 1 s for java. If durations are normal but the pool still drains as fast as it fills, demand exceeds capacity — bump `app.pool.targets.<language>` per [`../services/sandbox-manager.md#81-pool-empty-for--30-s`](../services/sandbox-manager.md#81-pool-empty-for--30-s):

```sh
# Edit /opt/oj/region.yml under oj-sandbox-manager.environment:
#   APP_POOL_TARGETS_PYTHON=4   # was 2
sudo docker compose -f /opt/oj/region.yml up -d oj-sandbox-manager
```

Sustained needs > 4 across languages on a 2-vCPU SPOT host is the signal to scale out — add a second compute VM (terraform module-level change).

### 4.4 Agent readiness timeout

If logs show `agent readiness timeout` consistently, the in-guest agent isn't printing its readiness line. The agent is at `infra/firecracker/agent/cmd/agent/main.go`; readiness is the line `agent listening on vsock:1234` (or equivalent). Causes:

- The rootfs was rebuilt with a stale agent binary. Bump `OJ_HARNESS_VERSION` env var on the SM (forces a fresh rootfs build via the startup script) and restart.
- The kernel image was swapped without the matching rootfs. The kernel must be compatible with the rootfs's libc.

```sh
# Force a fresh harness build
sudo sed -i 's/OJ_HARNESS_VERSION=.*/OJ_HARNESS_VERSION=v$(date +%s)/' /opt/oj/region.yml
sudo docker compose -f /opt/oj/region.yml up -d oj-sandbox-manager
```

### 4.5 Replenisher hung / no errors but pool stays empty

```sh
# Bounce the SM — clears any stuck replenisher state
sudo docker compose -f /opt/oj/region.yml restart oj-sandbox-manager

# Watch for the pool to refill
until [ "$(sudo docker exec oj-sandbox-manager sh -c \
    'curl -s localhost:9100/actuator/metrics/oj.sandbox.pool.ready' \
    | jq -r '.measurements[0].value // 0')" -ge 1 ]; do
  echo "[wait] pool not yet populated"; sleep 5
done
echo "[ok] pool repopulated"
```

The replenisher is a `@Scheduled(fixedDelayString = "${app.pool.replenish-interval-ms:1000}")` thread. A genuinely-stuck scheduler is rare; OOM on the SM JVM is the usual silent cause. Check `docker stats oj-sandbox-manager` — if memory is pegged at the limit, the JVM is GC-thrashing and the scheduler tick is starved.

---

## 5. Rollback / if mitigation went wrong

### 5.1 Bumped pool targets, host got OOM-killed

Each warm FC VM is ~128 MiB RAM. Targets of `{python:4, cpp:2, java:2}` = 8 VMs = ~1 GiB. On a 2 GB SPOT host with workers + SM + agent bridges, this is tight. If you raised targets and now `dmesg` shows OOM-kills of the SM, revert:

```sh
# Edit /opt/oj/region.yml — restore APP_POOL_TARGETS_* to 2/1/1
sudo docker compose -f /opt/oj/region.yml up -d oj-sandbox-manager
```

The right answer for sustained higher load is a second compute VM, not bigger pools on the same host.

### 5.2 Rebuilt rootfs, agent contract drifted

If the rebuilt rootfs broke the agent JSON contract (`vsock-client` request/response shape), every submission post-rebuild emits RUNTIME_ERROR. Don't keep the new rootfs. Roll back:

```sh
# Restore the previous rootfs from the OS image's snapshot (terraform-managed)
sudo cp /var/lib/firecracker/rootfs.ext4.bak /var/lib/firecracker/rootfs.ext4
sudo docker compose -f /opt/oj/region.yml restart oj-sandbox-manager
```

Today there's no automated `.bak` step — that's a roadmap gap. If you didn't take a manual backup before bumping `OJ_HARNESS_VERSION`, the only recovery is to check out the previous `infra/firecracker/rootfs/` files from git, redeploy, and re-run `build-rootfs.sh`.

### 5.3 Container won't start after restart

If `docker compose up -d oj-sandbox-manager` exits 125 or 126, it's almost always a missing volume mount or device. Cross-check against the repo's `infra/gcp/compose/region.yml::oj-sandbox-manager` block — that's the canonical shape (privileged, /dev/kvm, /tmp, /var/lib/firecracker, /sys/fs/cgroup).

---

## 6. Related incidents

- [`./firecracker-jailer-failure.md`](./firecracker-jailer-failure.md) — if FC won't boot at all (versus boots-but-too-slow). The two runbooks overlap on the "Process exited with code 1" log line; check both.
- [`../services/sandbox-manager.md#81-pool-empty-for--30-s`](../services/sandbox-manager.md#81-pool-empty-for--30-s) — the in-service runbook entry; covers the same ground with less prose.
- [`../services/execution-worker.md#82-submissions-piling-up-on-submissionspretest`](../services/execution-worker.md#82-submissions-piling-up-on-submissionspretest) — the visible upstream symptom (consumer lag). A pool-exhausted SM is the most common cause of worker lag.
- [`../tech-spec.md#11.2-spot-preemption-compute-vm`](../tech-spec.md#112-spot-preemption-compute-vm) — explains why post-preemption empty pools are expected for ~60 s.

---

## 7. Escalation

- After 5 minutes of `pool.ready=0` with the mitigation in §4 not converging, escalate to whoever owns the sandbox-manager service. Especially if KVM-availability errors persist after a container restart — this can mean the GCE VM's nested-virt is broken, which is recreation-only.
- Repeated FC `Process exited with code 1` paired with AppArmor / SELinux denial messages in `dmesg`: this is the firecracker-jailer-failure case; switch to that runbook.
- If the compute VM was preempted and `oj-compute` itself won't restart, that's a GCP-side issue — `gcloud compute instances start oj-compute --zone=asia-south1-a` and watch the startup script logs (`gcloud compute instances get-serial-port-output oj-compute --zone=asia-south1-a`).
- Auto-shutdown at 23:00 IST will stop both VMs cleanly. If a pool-empty alert fires near that boundary, confirm whether the auto-shutdown is the cause (`gcloud scheduler jobs list | grep oj-auto-shutdown`) before paging deeper.
