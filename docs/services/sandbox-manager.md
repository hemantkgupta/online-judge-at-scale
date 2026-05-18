# sandbox-manager

> **Owner page.** Last reconciled with the repo on **2026-05-18**.
>
> The single source of truth for the sandbox-manager service. Cross-cutting concerns (proto schema, Kafka topics, the auth model, the system-wide reliability story) live in [`../tech-spec.md`](../tech-spec.md). Forward-looking design lives in [`../design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md).
>
> Read this page if you are: (a) on-call for the sandbox layer, (b) onboarding into the team that owns it, (c) changing anything in `sandbox-manager/` or `infra/firecracker/`.

---

## 1. Purpose

Per-host privileged daemon. The trust-zone boundary that lets the execution-worker stay unprivileged — the worker has no `/dev/kvm`, no Firecracker binary, no rootfs mounts. Everything that requires elevated privilege for sandbox lifecycle (KVM, namespace creation, iptables, cgroups) lives here.

Three responsibilities, in priority order:

1. **VM lifecycle.** Spawn Firecracker processes, wait for the in-guest agent to come up, tear down dead VMs.
2. **Pool maintenance.** Keep N warm microVMs ready per language so the lease path doesn't pay cold-start cost.
3. **Lease / exec / release.** Hand a warm VM to one submission, enforce a wall-clock kill via the watchdog, clean up afterwards. The destroy-never-reuse invariant lives here: every lease destroys the VM after release.

The full architectural story (state machine, destroy-never-reuse rationale, egress lockdown) is in [`../tech-spec.md#6-sandbox-architecture-deep-dive`](../tech-spec.md#6-sandbox-architecture-deep-dive). This page is the operational and code-level reference.

---

## 2. External interfaces

### 2.1 REST API (consumed by the worker)

`POST /lease`

```http
POST /lease  HTTP/1.1
Content-Type: application/json

{
  "language": "python",            // python | java | cpp
  "submission_id": "<uuid>",       // for logging + watchdog tagging only
  "time_limit_ms": 1000,           // optional; per-problem wall clock for the watchdog
  "memory_limit_mib": 64           // optional; per-problem cgroup mem cap
}
```

Success (200):

```json
{
  "sandbox_id": "sb-2346...-9613",
  "vsock_uds_path": "/tmp/fc-sb-2346...-9613-vsock.sock",
  "vsock_port": 1234,
  "session_token": "<uuid>"
}
```

Failures:
- **503 `{error: "pool_exhausted", retry_after_ms: 250, language: "python"}`** — every warm VM is currently leased, replenishment is in flight. Worker treats this as transient (see [§8.5 of tech-spec](../tech-spec.md#85-pool-exhausted-retry-worker)).
- **500** — KVM unavailable, FC binary missing, cgroup setup failed. Operator-visible immediately.

`POST /release`

```http
POST /release  HTTP/1.1
Content-Type: application/json

{ "sandbox_id": "sb-2346...-9613" }
```

Returns 200 on success; 404 if the sandbox is already TERMINATED (idempotent). Triggers `forceKill` of the FC process, netns teardown, cgroup cleanup, iptables rule removal.

`POST /exec` (currently reserved)

The worker bypasses this and talks vsock directly via the Go `oj-vsock-client` bridge baked into its image. `POST /exec` is reserved for the "SM-as-proxy" architecture documented in the production blog at `/CSE-Raw/raw-blog/execution-service-gcp.md`. Don't depend on it; the implementation is a stub.

`GET /actuator/health`

Spring Boot actuator. Returns `{"status":"UP"}` once the FC binary is locatable, KVM is reachable, and the pool replenisher has booted at least one VM. Used by docker-compose's healthcheck and by future readiness probes.

### 2.2 Outbound

The SM does NOT call any HTTP service. Everything outbound is one of:

- **`firecracker --api-sock <path>`** — child process via `ProcessBuilder`. One per warm/leased VM. The PID is tracked in the `Sandbox` in-memory state.
- **`ip netns add/del/exec ...`** — shell-out for namespace lifecycle (egress lockdown).
- **`iptables -I/-D FORWARD/OUTPUT ...`** — host-side firewall rules (egress lockdown belt-and-suspenders).
- **`cgcreate / cgset`** — cgroup manipulation. Memory + CPU limits applied at lease.
- **Firecracker REST API over UDS** — once FC is up, the SM does `PUT /machine-config`, `PUT /drives/rootfs`, `PUT /vsock`, `PUT /actions {action_type: "InstanceStart"}` via the FC API socket. This is FC's own bootstrap protocol, not anything we designed.

### 2.3 Listening surface

- TCP `:9100` — the REST API (intra-VM only; the firewall rule `oj-allow-internal` permits 10.0.0.0/24).
- No public ingress.
- `/tmp/fc-sb-*-api.sock` — one Firecracker API socket per active VM. Permission 0600 owned by root.
- `/tmp/fc-sb-*-vsock.sock` — one vsock UDS per active VM, mounted into the worker container at `/tmp` via the compose bind mount.

---

## 3. Internal design

### 3.1 Pool state machine

Five states per sandbox; one `AtomicReference<SandboxState>` per `Sandbox`. The full diagram is in `../tech-spec.md#61-pool-state-machine`. Mechanics in this service:

| State | Entry trigger | Exit trigger | Owned by |
|---|---|---|---|
| PROVISIONING | replenisher `@Scheduled` tick discovers `pool.ready[lang] < target[lang]` | FC booted + agent printed its readiness line OR boot failed | `PoolManager` |
| READY | provisioning succeeded; sandbox enqueued onto the per-language ready deque | `POST /lease` pops it | `PoolManager` |
| LEASED | `LeaseService.lease()` rotates session token + applies cgroup + transitions | `POST /release` OR watchdog kill OR FC process exit | `LeaseService` |
| DIRTY | exit from LEASED for ANY reason; cleanup in progress | cleanup chain (kill FC, netns destroy, cgroup remove, UDS unlink) completes | `LeaseService.release` |
| TERMINATED | cleanup done | dropped from the active map by `sandboxes.remove(...)` | `LeaseService.release` |

Illegal transitions (e.g. LEASED → READY) throw `IllegalStateException` in `Sandbox.transition()`. The set of legal transitions is defined as a static map in `Sandbox.java`; modify it carefully.

### 3.2 Pool replenisher

A `@Scheduled(fixedDelayString = "${app.pool.replenish-interval-ms:1000}")` thread that reads the per-language target counts and target-ready deltas, then kicks off PROVISIONING for up to `app.pool.max-parallel-boot` (default 2) sandboxes per tick. The cap exists because a fresh FC boot pegs one vCPU for ~150 ms; concurrent unbounded provisioning thrashes the 2-vCPU compute VM.

Provisioning is best-effort: a single failure (FC crashes, KVM transient unavailable) is logged at ERROR but does not stop the next tick. After three consecutive failures for the same language the replenisher emits a warning log; alerting on this lives in OTel land (not yet wired).

Provisioning details:
1. Allocate a new `sandbox_id` (`sb-<uuid>`), a new context-ID (`cid`, monotonically incrementing for vsock), a placeholder session token.
2. Compute paths: `apiSock = /tmp/fc-<sandbox_id>-api.sock`, `vsockUds = /tmp/fc-<sandbox_id>-vsock.sock`.
3. Spawn `firecracker --api-sock <apiSock>` via `ProcessBuilder` with `redirectErrorStream(true)`.
4. `waitForApiSock(apiSock)` — polls every 25 ms up to 5 s for the FC API socket to appear.
5. `configureFirecracker(apiSock, vsockUds, cid, ...)` — issues the FC REST API calls to boot the VM: machine-config, drives, vsock, then `InstanceStart`. Removes the legacy `network-interfaces` block (§3.7).
6. Wait for the in-guest agent to print its readiness line on the host-side UDS (`waitForGuestAgent`, polls for 30 s with `app.agent.readiness-timeout-seconds`).
7. Add to the active map (`sandboxes.put(sandbox_id, sb)`), transition to READY, enqueue onto the per-language deque.

If any step fails after step 3, the FC process is `destroyForcibly()`'d, the UDS files unlinked, the sandbox removed from the map, and the slot returns to the "pool needs replenishment" state on the next tick.

### 3.3 LeaseService

The hot path. Acquires a warm sandbox from the pool, applies per-submission state, schedules the watchdog, returns the lease descriptor. The skeleton:

```java
public Sandbox lease(String submissionId, String language, ...) {
  Sandbox sb = poolManager.acquire(language);
  if (sb == null) throw new PoolExhaustedException(language, retryAfterMs);

  try {
    if (egressLockdownEnabled) {
      netnsApplier.create(sb.sandboxId());
      netnsApplier.installIptablesRules(sb.sandboxId());
    }
    sb.setSessionToken(UUID.randomUUID().toString());
    sb.setCodeDrivePath(codeDrivePath);
    sandboxes.putIfAbsent(sb.sandboxId(), sb);            // defence in depth
    cgroupApplier.applyAtLease(sb, mem, cpuQuota, cpuPeriod);
    sb.transition(SandboxState.LEASED);
    watchdog.schedule(sb.sandboxId(), wallClockMs, () -> forceKill(sb.sandboxId()));
    return sb;
  } catch (RuntimeException ex) {
    try { forceKill(sb.sandboxId()); } catch (Exception ignored) {}
    if (egressLockdownEnabled) {
      try { netnsApplier.removeIptablesRules(sb.sandboxId()); } catch (Exception ignored) {}
      try { netnsApplier.destroy(sb.sandboxId()); } catch (Exception ignored) {}
    }
    throw ex;
  }
}
```

Order matters: netns first (so a cgroup failure can still tear it down via the catch), then per-submission state, then cgroup, then state transition, then watchdog. The watchdog goes LAST because its callback assumes the sandbox is in LEASED.

`release()` is the reverse:

```java
public void release(String sandboxId) {
  Sandbox sb = sandboxes.remove(sandboxId);
  if (sb == null) { log.warn(...); return; }     // already terminated; idempotent
  watchdog.cancel(sandboxId);                     // cancel before destroying state
  sb.transition(SandboxState.DIRTY);
  try { killByPid(sb.firecrackerPid()); } catch (Exception ex) { log.warn(...); }
  cgroupApplier.cleanup(sb);
  tryDelete(sb.apiSockPath());
  tryDelete(sb.vsockUdsPath());
  if (egressLockdownEnabled) {
    try { netnsApplier.removeIptablesRules(sb.sandboxId()); } catch (Exception ignored) {}
    try { netnsApplier.destroy(sb.sandboxId()); } catch (Exception ignored) {}
  }
  sb.transition(SandboxState.TERMINATED);
}
```

The `tryDelete` calls are `Files.deleteIfExists` — never throw. The `catch (Exception ignored)` blocks around netns ops are deliberate: a fail-during-cleanup must not leak the FC process, so cleanup walks every step regardless.

### 3.4 WatchdogService

Per-lease wall-clock kill. Backed by a `ScheduledExecutorService` with thread pool size `app.watchdog.pool-size` (default 4). On `schedule(sandboxId, fireAtMs, callback)`:

1. Cancels any pre-existing future for the same `sandboxId` (idempotent — release races with watchdog scheduling).
2. Submits a delayed task at `fireAtMs - now()`. Minimum delay clamped to 100 ms.
3. Stores the future in `Map<String, ScheduledFuture<?>> futures`.

On `cancel(sandboxId)`: removes the future from the map + calls `.cancel(false)`. Called by `release()` to prevent the watchdog firing on an already-released sandbox.

The callback is always `() -> forceKill(sb.sandboxId())`. `forceKill` is the same teardown path as `release` minus the worker-initiated 200 OK; the sandbox goes DIRTY → TERMINATED and the worker sees its vsock connection drop. The worker maps this to TIME_LIMIT_EXCEEDED based on whether the agent's response contains a "timeout" verdict for any ordinal — debatable; see [§14 of tech-spec](../tech-spec.md#14-known-limitations-and-debt).

### 3.5 CgroupApplier

Per-lease memory + CPU enforcement via cgroups v1 (the compute VM runs Ubuntu 22.04 with systemd cgroup v1 backend). On `applyAtLease(sb, memBytes, cpuQuotaUs, cpuPeriodUs)`:

1. `cgcreate -g memory,cpu:oj/sb-<id>` — creates the cgroup hierarchy under both controllers.
2. `cgset -r memory.limit_in_bytes=<bytes> oj/sb-<id>` — hard memory cap.
3. `cgset -r cpu.cfs_quota_us=<quota> oj/sb-<id>` / `cpu.cfs_period_us=<period>` — CPU fair-share.
4. `echo <firecracker_pid> > /sys/fs/cgroup/memory/oj/sb-<id>/cgroup.procs` (and same for cpu) — pin the FC process. Children inherit.

Memory limit comes from the per-problem `memory_limit_mib` (problem-service plumbs it through; see [tech-spec §2.3](../tech-spec.md#23)). CPU quota/period default to 100,000 / 100,000 = 1 vCPU — overridable per-lease via the same plumbing.

`cleanup(sb)`: `cgdelete -g memory,cpu:oj/sb-<id>`. Idempotent; logs at WARN on missing cgroup.

Test-time seam: `CgroupApplier` takes a `Function<List<String>, Process>` factory in its constructor. Unit tests substitute it to capture argv without actually invoking `cgcreate`.

### 3.6 NetnsApplier

Per-microVM Linux network namespace + host-side iptables. Full design in [`../design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md). Surface:

```java
public interface NetnsApplier {
  void create(String sandboxId);
  void destroy(String sandboxId);
  String netnsName(String sandboxId);              // trims to 15 chars (Linux netns limit)
  List<String> execPrefix(String sandboxId);       // ["ip","netns","exec","oj-<id>"]
  void installIptablesRules(String sandboxId);     // no-op when iptables-enabled=false
  void removeIptablesRules(String sandboxId);
}
```

`create(sandboxId)`: `ip netns add oj-<id-prefix>`. The netns has only `lo` (loopback); no NICs. When FC is exec'd inside it via `execPrefix`, the FC binary's `open()` of the tap device fails (no tap exists in this netns) and the microVM boots with zero virtio-net devices.

`installIptablesRules(sandboxId)`:
1. `iptables -I FORWARD -i fc-tap-<id> -j DROP` — if any tap interface re-appears, drop forwarding traffic.
2. `iptables -I OUTPUT -m owner --uid-owner firecracker -j REJECT` — the FC process can't `bind()` to any host network interface.

Cleanup uses `-D` with identical argv. The rules are inserted at the top (`-I`) so they take precedence over any other host rules.

Defaults: `app.sandbox.egress-lockdown.enabled=true`, `iptables-enabled=false` on dev hosts (macOS has no `iptables`). Production GCP host has both enabled.

### 3.7 FirecrackerLauncher

Builds the Firecracker machine-config JSON + invokes the FC API. Two responsibilities:

1. **machine-config.** Today emits ONLY `{"vcpu_count": N, "mem_size_mib": M}`. The legacy `network-interfaces` block was removed in commit `244449b` so the microVM boots with zero NICs (egress lockdown depends on this). The `drives` block continues to provide the rootfs.
2. **argv assembly.** `buildFirecrackerArgv(sandboxId, apiSock)` returns `["ip", "netns", "exec", netnsName, "firecracker", "--api-sock", apiSock]` when egress lockdown is enabled, falling back to `["firecracker", "--api-sock", apiSock]` when it isn't. The `ip netns exec` prefix is what plants the FC process inside the sealed namespace.

### 3.8 vsock bridge (oj-vsock-client)

A ~250-line Go binary baked into the worker image at `/usr/local/bin/oj-vsock-client`. Source at `infra/firecracker/agent/cmd/vsock-client/main.go`. Used by the worker to talk to the in-guest agent without needing AF_VSOCK in the JVM (the JVM has no native vsock support).

Protocol:
1. Connect to the host-side UDS at `<vsock_uds_path>`.
2. Write `CONNECT <port>\n` (FC's vsock connect protocol; port is always 1234, the agent's listening port).
3. Expect `OK <local_port>\n` from FC. If FC writes anything else, fail.
4. Forward stdin → UDS, UDS → stdout.

The worker shells out via `ProcessBuilder` with the JSON request piped to stdin and reads the JSON response from stdout. **Critical regression footgun**: Firecracker's vsock does NOT preserve AF_UNIX half-close semantics. Calling `(*net.UnixConn).CloseWrite()` after sending the request tears down the FC vsock layer entirely, killing the agent's response write with `broken pipe`. The bridge deliberately does NOT half-close; the agent's JSON framing serves as the end-of-request signal. Don't reintroduce CloseWrite.

---

## 4. Data ownership

The SM is **stateless across restarts**. All state is in-memory or in ephemeral filesystem locations:

| State | Lifetime | Location |
|---|---|---|
| `sandboxes` map (active VMs) | process lifetime | JVM heap (`ConcurrentHashMap<String, Sandbox>`) |
| Per-language ready deques | process lifetime | JVM heap (`PoolManager`'s per-language `Deque<Sandbox>`) |
| Watchdog futures | per-lease | JVM heap (`WatchdogService.futures`) |
| FC API sockets | per-VM (created at boot, deleted at release) | `/tmp/fc-<sandbox_id>-api.sock` |
| vsock UDS files | per-VM | `/tmp/fc-<sandbox_id>-vsock.sock`. Mounted into the worker container via the compose bind `- /tmp:/tmp`. |
| iptables rules | per-lease (when enabled) | host iptables (FORWARD + OUTPUT chains) |
| netns entries | per-lease (when enabled) | `/var/run/netns/oj-<id-prefix>` |
| cgroups | per-lease | `/sys/fs/cgroup/{memory,cpu}/oj/sb-<id>/` |

The SM does NOT touch CRDB, Kafka, Redis, or GCS. It is a sandbox lifecycle service only. The worker holds the per-submission durable state (idempotency keys in CRDB).

If the SM process is restarted (deploy, crash), every active sandbox is lost — there's no recovery. The FC processes are orphaned (PID 1 still kicks them on next reboot via `tini` semantics in the rootfs); their UDS files are deleted by `oj-control-plane.service`'s pre-start cleanup. Worker idempotency tolerates this: re-leased submissions reclaim their `processing` rows after the 300 s stale window.

---

## 5. Failure modes

| Failure | Detection | Behaviour |
|---|---|---|
| `/dev/kvm` missing or permission-denied | SM startup log "KVM not available" | All `/lease` requests return 500. Operator-visible immediately. Caused by host kernel without KVM (nested-virt not enabled on the GCE VM) OR by running the container without `--privileged`. |
| FC binary not at `app.firecracker.binary` | SM startup log "firecracker binary not found" | All `/lease` requests return 500. Caused by a corrupted compose mount OR a fresh VM whose startup-script didn't run `build-rootfs.sh` yet. |
| Rootfs file missing | SM startup log "rootfs not found" | All `/lease` requests return 500. Same recovery as FC-binary missing. |
| Pool empty (all warm VMs leased) | `PoolManager.acquire()` returns null | `/lease` returns 503 `pool_exhausted` with `retry_after_ms`. Worker treats as transient. Replenisher catches up. |
| FC process crashes mid-provisioning | `Process.waitFor` returns non-zero OR `waitForApiSock` times out | Sandbox transitions PROVISIONING → TERMINATED. Slot returns to the replenisher's queue. Logged at ERROR. Three consecutive failures in 5 s for the same language emit a louder warning. |
| FC process crashes mid-lease | Worker's vsock connection drops; SM's watchdog observes the FC PID is gone | The release path runs as if `/release` was called (forceKill is idempotent on a dead process). The worker emits `INTERNAL_ERROR` mapped to `RUNTIME_ERROR` for the contestant. |
| Watchdog fires (wall-clock exceeded) | `ScheduledExecutorService` triggers `forceKill` | Same teardown as release. Worker maps the dropped connection to `TIME_LIMIT_EXCEEDED` if the agent didn't get a chance to write back. |
| Agent dies mid-execution | Worker's vsock-client sees `broken pipe` | Worker maps to `RUNTIME_ERROR`. The microVM is destroyed; replenisher fills in. |
| `/release` for unknown sandbox | `sandboxes.remove()` returns null | 200 OK + WARN log. Idempotent — worker may call release twice (once in finally block, once via watchdog timeout). |
| Cgroup setup fails | `cgcreate` exits non-zero | The lease throws RuntimeException; the catch block destroys the half-initialised sandbox. Caused by `cgroup-tools` not installed OR `oj/` parent cgroup missing (created at SM boot; survives container restart because cgroups are kernel-level). |
| Netns create fails | `ip netns add` exits non-zero | Same as cgroup. Most common cause: dev host without `iproute2` installed; production GCE has it. |
| iptables rule already exists | `iptables -I` returns 0 even with duplicate | Idempotent insert — `-I` inserts at top regardless of existing rules. Cleanup uses `-D` which removes by argv match. |
| Process limit reached (FC processes pile up) | `Too many open files` errors | Most often caused by the pool max-parallel-boot being set too high vs the systemd LimitNOFILE on the compute VM. Default LimitNOFILE on Ubuntu is 65535; pool depth 4 + 30 s watchdog never approaches it. |

---

## 6. Configuration reference

All properties live in `sandbox-manager/src/main/resources/application.yml`; overridable via environment variables (Spring relaxed binding maps `APP_FIRECRACKER_BINARY` → `app.firecracker.binary`). Defaults shown.

| Property | Default | Purpose |
|---|---|---|
| `app.firecracker.binary` | `/usr/local/bin/firecracker` | Path to the FC executable. |
| `app.firecracker.kernel-image` | `/var/lib/firecracker/vmlinux` | Guest kernel image. Built once on the host via `infra/firecracker/rootfs/build-rootfs.sh`. |
| `app.firecracker.rootfs-image` | `/var/lib/firecracker/rootfs.ext4` | Guest rootfs. |
| `app.firecracker.api-sock-dir` | `/tmp` | Where per-VM FC API sockets live. |
| `app.firecracker.vsock-uds-dir` | `/tmp` | Where per-VM vsock UDS files live. Must be the same volume the worker container has bind-mounted. |
| `app.agent.vsock-client` | `/usr/local/bin/oj-vsock-client` | Path to the Go bridge binary baked into the worker image. |
| `app.pool.targets.python` | `2` | Warm-VM target for Python. |
| `app.pool.targets.cpp` | `1` | C++ target. |
| `app.pool.targets.java` | `1` | Java target. |
| `app.pool.max-parallel-boot` | `2` | Cap on concurrent FC spawns per replenisher tick. Prevents 2-vCPU thrashing. |
| `app.pool.replenish-interval-ms` | `1000` | How often the replenisher tick fires. |
| `app.lease.wall-seconds` | `30` | Watchdog kill deadline. |
| `app.watchdog.pool-size` | `4` | Thread-pool size for `ScheduledExecutorService`. |
| `app.agent.readiness-timeout-seconds` | `30` | How long PROVISIONING waits for the agent's readiness line. |
| `app.sandbox.egress-lockdown.enabled` | `true` | Master switch for netns + iptables. |
| `app.sandbox.egress-lockdown.iptables-enabled` | `false` | Default false for macOS / CI; flip true on production hosts. |
| `app.sandbox.egress-lockdown.ip-binary` | `/usr/sbin/ip` | Path to `ip` for netns ops. |
| `app.sandbox.egress-lockdown.iptables-binary` | `/usr/sbin/iptables` | Path to `iptables`. |
| `app.region` | `${REGION:-asia-south1}` | Stamped onto metrics; not behaviourally meaningful (no per-region pools yet). |

OpenTelemetry env vars are documented in [`../tech-spec.md#92-service-side-activation`](../tech-spec.md#92-service-side-activation) — same posture as every other JVM service (default disabled, operator flips on after collector is healthy).

---

## 7. Metrics emitted

The `SandboxMetrics` Spring bean wires these via the OpenTelemetry API (registered eagerly so the names exist even when the agent is disabled). All metric names are prefixed `oj.sandbox.*`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `oj.sandbox.lease.latency_seconds` | histogram | `language` | Wall-clock from `/lease` request → response. Includes pool-acquire + cgroup + watchdog setup. Should be < 50 ms p99. |
| `oj.sandbox.pool.ready` | gauge | `language` | Current count of READY sandboxes per language. Should equal target in steady state; below target = replenisher is behind. |
| `oj.sandbox.leases.active` | gauge | `language` | Current count of LEASED sandboxes. Incremented in `lease()`, decremented in `release()` and `forceKill`. |
| `oj.sandbox.pool.exhausted_total` | counter | `language` | Increment each time `/lease` returns 503. Sustained > 0 → pool sizing is too low for the offered load. |
| `oj.sandbox.watchdog.fired_total` | counter | `language` | Watchdog killed a sandbox. Healthy systems have very few of these (only genuine TLE). High count → contestant code is hitting the wall clock OR pool replenishment is slow enough that the wall-clock expires before exec finishes. |
| `oj.sandbox.provision.duration_seconds` | histogram | `language` | Time from PROVISIONING → READY. Should be < 500 ms for python/cpp, < 1 s for java (JVM warm-up). |
| `oj.sandbox.provision.failure_total` | counter | `language`, `reason` | FC boot failed; reason is one of `api_sock_timeout`, `agent_readiness_timeout`, `firecracker_exit`. |

The planned per-language pool depth dashboard surfaces three of these together (ready / active / exhausted) per language; see [`../design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md#dashboards).

---

## 8. Runbook

Common incidents and the first diagnostic step + fix. Pages are deliberately short; assume the on-caller has SSH + `docker logs` access.

### 8.1 "Pool empty for > 30 s" (alert)

**Symptom.** `oj.sandbox.pool.ready{language=X} == 0` for 30 s. Lease API returns 503 to the worker.

**Diagnose.**
```sh
gcloud compute ssh oj-compute --zone=asia-south1-a --tunnel-through-iap --command='
  sudo docker logs oj-sandbox-manager --tail 200 | grep -E "POOL provision failed|PoolExhausted"
  sudo docker exec oj-sandbox-manager sh -c "curl -s localhost:9100/actuator/health"
'
```

**Likely causes & fixes.**
- *Replenisher crashed.* Restart the SM container. Check FC binary + kernel + rootfs paths.
- *KVM saturated by SPOT preemption.* If `oj-compute` was preempted and restarted, it can take ~2 min for the pool to fill. Verify with `oj.sandbox.provision.duration_seconds` — should be normal post-boot.
- *Pool target too low for offered load.* Bump `app.pool.targets.<language>` in compose env + recreate the SM container. Sustained needs > 4 across languages → scale to a second compute VM.

### 8.2 "Lease API returning 503 continuously"

**Symptom.** Worker logs `[firecracker] sandbox-manager call failed ... HTTP 503 pool_exhausted` for every submission.

**Same diagnose flow as 8.1.** If `pool.ready > 0` but the worker still sees 503, the worker is asking for a *different* language than what's warm. Verify the worker's language stamp matches a configured pool target.

### 8.3 "Watchdog firing on every lease (cold-start regression)"

**Symptom.** `oj.sandbox.watchdog.fired_total` spikes; every submission gets TIME_LIMIT_EXCEEDED including trivial ones.

**Diagnose.**
```sh
sudo docker logs oj-sandbox-manager --tail 100 | grep -E "watchdog FIRED|forceKill"
# Check provisioning time:
sudo docker logs oj-sandbox-manager --tail 200 | grep "POOL-READY" | head -5
```

**Likely causes & fixes.**
- *Provisioning regression.* `provision.duration_seconds` is > `app.lease.wall-seconds`, so the watchdog fires before the agent even gets the request. Look for a recent rootfs change (`OJ_HARNESS_VERSION` bump?) or kernel change. Roll back.
- *Wall-clock too tight for legitimate problems.* The default 30 s is hardcoded — should be per-problem (see [tech-spec §2.3](../tech-spec.md#23) plumbing). Until that lands, bump `app.lease.wall-seconds` temporarily.

### 8.4 "Firecracker jailer chroot fails"

**Symptom.** SM logs `Process exited with code 1` immediately after spawning FC. `dmesg` on the host shows AppArmor / SELinux denial.

**Fix.** The compose runs the SM container `privileged: true` and mounts `/dev/kvm` directly. If a host-side mandatory access control policy was added (some GCE custom images ship AppArmor profiles for Docker), the container needs an additional `--security-opt seccomp=unconfined` or a custom AppArmor profile. Document the host's MAC policy in the runbook before tightening.

### 8.5 "iptables rules accumulating"

**Symptom.** `iptables -L FORWARD -n | grep fc-tap | wc -l` returns a number that grows without bound across restarts.

**Cause.** A previous SM crash skipped the cleanup path. Rules from dead sandboxes persist.

**Fix.** One-time cleanup:
```sh
sudo iptables -S FORWARD | grep 'fc-tap-' | sed 's/^-A/-D/' | xargs -L1 sudo iptables
sudo iptables -S OUTPUT  | grep 'firecracker' | sed 's/^-A/-D/' | xargs -L1 sudo iptables
```

Long-term fix: a host-side systemd unit that flushes orphan rules on boot. Roadmap item; not yet implemented.

### 8.6 "/dev/kvm permission denied"

**Symptom.** SM startup log `open /dev/kvm: permission denied`.

**Fix.** Confirm the compose entry has `privileged: true` AND `devices: - /dev/kvm:/dev/kvm`. On a hardened host the `kvm` group ownership matters; the FC docs cover it.

---

## 9. Tests & verification

### 9.1 Unit tests (`sandbox-manager/src/test/java/`)

| File | Coverage |
|---|---|
| `CgroupApplierTest` | argv shape for `cgcreate`/`cgset`/`cgdelete`; idempotent cleanup; failure surfaces as RuntimeException |
| `NetnsApplierTest` | argv shapes (`ip netns add/del/exec`); idempotent destroy on missing netns; iptables enabled/disabled paths; netns name 15-char trim |
| `LeaseServiceEgressLockdownTest` | Lockdown enabled: `create` + `installIptablesRules` called once on lease; `destroy` + `removeIptablesRules` called once on release; mid-flight failure cleans up via `atLeastOnce` (catch-path + forceKill→release path both acceptable) |
| `PoolManagerTest` | Acquire returns null when queue empty; targets per language; replenishment delta calculation |
| `WatchdogServiceTest` | Schedule + cancel; double-cancel idempotent; pool sizing |
| `SandboxControllerTest` | REST surface — happy path lease/release; 503 on pool exhausted; 404 on release-unknown |

Run via `./gradlew :sandbox-manager:test`. Full repo test (`./gradlew test`) includes this in the matrix.

### 9.2 Integration tests

**Go egress integration** at `infra/firecracker/agent/cmd/agent/egress_test.go`, build tag `//go:build integration_microvm`. Asserts `net.Dial("tcp", "1.1.1.1:80")` fails within 250 ms from inside a locked-down microVM. Run:

```sh
GOOS=linux GOARCH=amd64 go test -tags=integration_microvm \
  -c -o /tmp/egress_test \
  ./infra/firecracker/agent/cmd/agent
# scp /tmp/egress_test into a microVM rootfs and exec it; exit 0 means lockdown holding.
```

### 9.3 Manual smoke

Direct REST against a running SM:
```sh
# Lease
curl -sX POST http://localhost:9100/lease -H 'Content-Type: application/json' \
  -d '{"language":"python","submission_id":"manual-test-1","time_limit_ms":1000}'
# → { "sandbox_id": "sb-...", "vsock_uds_path": "/tmp/...", ... }

# Verify the netns exists (when egress lockdown enabled)
sudo ip netns list | grep oj-

# Release
curl -sX POST http://localhost:9100/release -H 'Content-Type: application/json' \
  -d '{"sandbox_id":"sb-..."}'
```

End-to-end through the worker is documented in [`../tech-spec.md#132-smoke-tests`](../tech-spec.md#132-smoke-tests).

---

## 10. Relevant design docs

- [`../design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md) — full design rationale for §3.6 + §3.7 (netns, iptables, the FC machine-config strip). Roadmap §3.1.
- [`../design-docs/kafka-cluster-and-crdb-cluster.md`](../design-docs/kafka-cluster-and-crdb-cluster.md) — SM doesn't talk to Kafka or CRDB, but the 3-broker / 3-node migration affects what assumptions the worker can make about backpressure (which feeds into the pool sizing analysis here).

The OTel collector deployment doc affects the metrics here once the agent is activated; see `../design-docs/otel-collector-deployment.md`.

---

## 11. Code map

| Concern | File |
|---|---|
| REST endpoints | `sandbox-manager/src/main/java/com/onlinejudge/sandbox/web/SandboxController.java` |
| Lease + release | `.../service/LeaseService.java` |
| Pool management | `.../service/PoolManager.java` |
| Watchdog | `.../service/WatchdogService.java` |
| Cgroups | `.../service/CgroupApplier.java` |
| Netns + iptables | `.../service/NetnsApplier.java` |
| FC machine-config + argv | `.../firecracker/FirecrackerLauncher.java` |
| Sandbox state type | `.../model/{Sandbox,SandboxState}.java` |
| Metrics | `.../metrics/SandboxMetrics.java` |
| Go vsock bridge (worker-side, baked into worker image) | `infra/firecracker/agent/cmd/vsock-client/main.go` |
| In-guest agent (PID 1) | `infra/firecracker/agent/cmd/agent/main.go` |
| Rootfs builder | `infra/firecracker/rootfs/build-rootfs.sh` |
| PID-1 init script | `infra/firecracker/rootfs/init.sh` |
