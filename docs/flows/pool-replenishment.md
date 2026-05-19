# Sandbox Warm-Pool Replenishment

> Last reconciled with the repo on 2026-05-19.
>
> How sandbox-manager keeps a steady supply of warm Firecracker microVMs ready for `/lease` calls without paying the ~125 ms boot penalty on the contestant's hot path.

## 1. Why this flow exists

A microVM boot takes ~125 ms. A contest submission's pretest verdict SLA is sub-2 s end-to-end. If `/lease` blocked on a fresh boot, ~6% of the SLA budget would burn before code ever ran. The warm-pool design pays the boot latency in the background — `/lease` becomes a constant-time pool dequeue.

## 2. Pool state machine

Per language (python / java / cpp), sandbox-manager maintains a pool of microVMs. Each VM transitions through:

```
            spawn requested
EMPTY ─────────────────────────► SPAWNING
                                    │  Firecracker exec(jailer + kernel + rootfs)
                                    ▼
                                 BOOTING
                                    │  agent reports vsock-ready
                                    ▼
                                  READY ◄───── pool-dequeue ───── /lease ──► LEASED
                                                                                │
                                                                                ▼
                                                                          (contestant code runs)
                                                                                │
                                                              /release ─────────┘
                                                                                │
                                                                                ▼
                                                                            DESTROYED
                                                                                │
                                                                                ▼ (spawner observes deficit)
                                                                              EMPTY (slot reclaimed)
```

Key rules:
- A VM is **never recycled** after release. Always destroyed. Same submission → same VM is acceptable; different submission → fresh VM. See [ADR-0004](../adr/0004-firecracker-over-docker-for-prod.md).
- Target pool size per language is configured in `app.pool.targets.<lang>` (default 4 per language for python/java, 2 for cpp).
- The background spawner runs every 250 ms, observes `(target - current_ready - current_spawning)`, and spawns up to N VMs to close the gap.

## 3. Step-by-step walkthrough

1. **Spawner tick.** `sandbox-manager/src/main/java/com/onlinejudge/sandboxmgr/pool/PoolManager.java#tick`. Reads the current `(language → state count)` snapshot. For each language, computes the deficit. Submits up to `spawn_concurrency` VM-spawn jobs to a fixed-size executor (cap is `app.pool.spawn-concurrency`, default 4 — chosen to prevent a deficit cascade from saturating the KVM ioctl path).

2. **VM spawn.** `FirecrackerLauncher.spawn(language)`:
   - Allocates a per-VM tap interface in a fresh Linux network namespace with no external interfaces — only loopback. See [`design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md).
   - Allocates a vsock UDS path: `/tmp/fc-<uuid>-vsock.sock`.
   - Writes the Firecracker config JSON (`fc-config-<uuid>.json`) with the kernel image, rootfs path (per-language), vsock CID, memory limits, vCPU count.
   - Forks `jailer` which forks Firecracker. Records the PID and the API socket path.
   - VM transitions to BOOTING.

3. **In-guest agent boots.** The rootfs's `/init` script execs the Go agent at PID 1. The agent immediately:
   - Opens a vsock listener on the CID/port pair.
   - Sends a "ready" UDP/vsock heartbeat to the host's listener.
   - Awaits a JSON request on stdin (well, on vsock — but the wire protocol is the same).

4. **Pool registration.** `PoolManager` receives the ready heartbeat (or polls the vsock listener at 50 ms intervals). VM transitions BOOTING → READY. Pool depth increments. The VM is now in the `available` queue keyed by language.

5. **`/lease` dequeue.** `LeaseController.lease(LeaseRequest)`:
   - Looks up the language's available queue.
   - If empty → return 503 with `pool_exhausted` and `retry_after_ms = <target boot time>`. See [`pool-exhausted-backpressure.md`](./pool-exhausted-backpressure.md).
   - Otherwise dequeue one VM, transition READY → LEASED, return `{sandboxId, vsockUdsPath, port}` to the worker.
   *Invariant:* a `LEASED` VM is owned by exactly one worker thread until matching `/release` or watchdog kill.

6. **Watchdog.** `WatchdogService` runs every 100 ms. For each LEASED VM, checks `leasedAt + app.lease.wall-seconds > now()`. If exceeded → forcibly kill the Firecracker PID via SIGKILL, mark the VM as needing reclamation. This catches buggy/hostile contestant code that hangs the agent.

7. **Release.** `LeaseController.release(...)`:
   - Marks the VM DESTROYED, kills the Firecracker process, removes the vsock UDS file, releases the tap interface, releases the network namespace.
   - The deletion of this VM IS the signal to the spawner that a deficit exists; the next tick spawns a replacement.

## 4. Failure modes at each step

| Step | Failure | Detection | Behaviour |
|---|---|---|---|
| 2 | KVM ioctl rejected (host out of FDs / cgroups exhausted) | exception in `FirecrackerLauncher` | log; back off; pool stays short; eventually `/lease` returns 503 |
| 2 | Per-VM tap creation fails | netlink errno | same — back off; pool stays short |
| 3 | Agent fails to send ready heartbeat within `app.boot.timeout-seconds` (default 8) | timeout in PoolManager | SIGKILL the VM; record `boot_timeout_total` metric; spawn replacement |
| 5 | `pool_exhausted` on `/lease` | length of `available` queue | 503 with `retry_after_ms = max(remaining boot time, 200ms)`; worker nacks |
| 6 | Watchdog kill while agent was mid-exec | SIGKILL during exec | worker's `AgentClient.exec` returns parse error / process exit; mapped to TLE or INTERNAL_ERROR |
| 7 | Release called for unknown sandboxId | not found | 404; logged; no state change |
| - | Disk full on the host (rootfs path) | `ENOSPC` on Firecracker spawn | spawner backs off; pool degrades; alert fires |
| - | KVM kernel module unavailable (e.g. nested-virt VM lost it) | `/dev/kvm` not openable | the SM Docker fallback for dev is activated (Linux only); on macOS, the dev compose already uses Docker backend |

## 5. Sizing observations

The pool size × boot time ≈ the longest acceptable burst-vs-baseline ratio. At target=4 with ~150 ms boot, the system absorbs a 4-VM burst over baseline without `/lease` ever blocking; sustained submission rate must stay below `(1 / boot_time) × spawn_concurrency` per language or the pool depletes monotonically. Production target metric `sandbox_pool_ready{language=X}` should stay ≥ 2 for 99.9% of the contest minute.

## 6. Related material

- Lockdown details: [`design-docs/microvm-egress-lockdown.md`](../design-docs/microvm-egress-lockdown.md).
- Pool exhausted backpressure (the contestant-facing side): [`pool-exhausted-backpressure.md`](./pool-exhausted-backpressure.md).
- Owner page: [`services/sandbox-manager.md`](../services/sandbox-manager.md).
- ADR: [`adr/0004-firecracker-over-docker-for-prod.md`](../adr/0004-firecracker-over-docker-for-prod.md).
