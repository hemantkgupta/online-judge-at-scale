# First 30 minutes with this repo

> A guided tour for a Principal Engineer who just cloned the repo and wants to understand what they're looking at.
>
> Last reconciled with the repo on 2026-05-19.

## What you'll have at the end of 30 minutes

- A green test suite locally.
- The full stack running under Docker Compose.
- A submission you sent yourself returning ACCEPTED.
- A mental model of the 8 services and where they live in the repo.
- A short list of next docs to read, depending on what you came here to do.

The 30-minute target is a budget, not a stopwatch. If you skim, you'll finish in 20. If you read carefully, 45. Both are fine.

## 5 minutes — what is this?

This repo is the companion implementation for the system-design blog at `raw-blog/online-judge-at-scale.md` (in the parent `CSE-Raw` repo). It's a multi-service online judge that accepts contestant code over HTTP, runs it inside a hardware-isolated Firecracker microVM against a private test suite, and publishes the verdict over Kafka. Downstream consumers update a sharded Redis leaderboard, compute final scores in Flink, and stream results back to the React SPA over STOMP-over-SockJS.

The architecture is event-driven on a Kafka spine, with eight Spring services + a Flink job + a React SPA. Read [`docs/tech-spec.md` §1](./tech-spec.md) for the canonical overview.

## 10 minutes — get it running locally

### Prerequisites

- JDK 17+
- Docker Desktop (macOS) or Docker Engine + Compose plugin (Linux). On Linux you'll also get the Firecracker backend if `/dev/kvm` is available.
- ~8 GB free RAM for the compose stack.

### Steps

```sh
cd online-judge-at-scale

# 1) Verify the test suite is green.
./gradlew test

# 2) Bring up the infrastructure layer.
docker compose up -d

# 3) Check what's up.
docker compose ps
```

The compose file boots the **infrastructure** containers — Kafka (broker + a second `kafka-global` instance for cross-region replication tests), Zookeeper (for the local stepping-stone), CockroachDB, Redis (primary + replica), ClickHouse, Flink (jobmanager + taskmanager), and the OTel Collector. The Spring services themselves are **not** in compose by default; run them with Gradle:

```sh
# In separate terminals, or as background jobs.
./gradlew :api-gateway:bootRun
./gradlew :execution-worker:bootRun
./gradlew :sandbox-manager:bootRun
./gradlew :problem-service:bootRun
./gradlew :contest-service:bootRun
./gradlew :leaderboard-service:bootRun
# The frontend dev server:
cd frontend && npm install && npm run dev
```

The SPA dev server runs on `http://localhost:5173` (Vite default). The api-gateway listens on `http://localhost:8080`. CockroachDB's web UI is at `http://localhost:18080`. Kafka's broker is at `localhost:9092`.

> **macOS sandboxing gap.** Firecracker requires `/dev/kvm`, which macOS doesn't expose. On macOS the execution-worker falls back to `DockerExecutionService` (set `app.sandbox.backend=docker` in `execution-worker/src/main/resources/application.yml`). The security boundary is weaker; the integration shape is the same.

## 10 minutes — submit your first problem and trace it

There's a reference smoke problem `sum-of-two` at `infra/firecracker/test/problems/sum-of-two/` with 5 test cases and reference solutions in Python, Java, and C++ (plus a deliberately-wrong Python). A submit harness lives at `infra/firecracker/test/submit-sample.py`.

```sh
# Submit a known-good Python solution.
python3 infra/firecracker/test/submit-sample.py \
    --problem sum-of-two \
    --language python \
    --solution infra/firecracker/test/problems/sum-of-two/solutions/sum.py \
    --expect-verdict ACCEPTED
```

While that runs, watch the execution-worker log to see the pipeline traverse:

```sh
./gradlew :execution-worker:bootRun  # in one terminal
```

You should see lines in this order (search for your submission ID):

```
[worker:pretest] Received submission=<id>
[firecracker|docker] LEASED submission=<id> sandbox=<sbox-id>
[firecracker|docker] submission=<id> overall=OK perTest=5
[worker:pretest] Verdict submission=<id> result=ACCEPTED time=66ms
[worker:pretest] Submission <id> accepted; enqueued to submissions.system for Phase 2
```

If you have the SPA running and you submitted via the SPA UI rather than the harness, the verdict appears within a second as a STOMP frame — the pill flips from "Pending" to "ACCEPTED" without a page refresh.

For the full trace of every hop in the pipeline, open [`flows/submission-roundtrip.md`](./flows/submission-roundtrip.md).

## 5 minutes — the mental model

```
                  ┌─────────────────────────────────────┐
                  │           React SPA (Vite)          │
                  └──────────────┬──────────────────────┘
                                 │ REST + STOMP/SockJS
                                 ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                       api-gateway                           │
   │   auth, submission ingest (outbox), reconciliation, schema  │
   └──────────────┬──────────────────────────────┬───────────────┘
                  │                              │
                  │ CRDB tx (subs + outbox)      │ REST → other services
                  ▼                              │
              CockroachDB                        │
                  │ changefeed                   │
                  ▼                              │
                Kafka ──────── topics ───────────┤
                  │                              │
   ┌──────────────┴───────────┐                  │
   │                          │                  │
   ▼                          ▼                  │
execution-worker      leaderboard-service        │
   │                          ▲                  │
   │ /lease,/release          │ STOMP push       │
   ▼                          │                  │
sandbox-manager        ◀──── verdict ◀──── execution-worker
   │
   ▼ KVM
Firecracker microVMs (one per submission)
```

| Service | Directory | One-line role |
|---|---|---|
| **api-gateway** | `api-gateway/` | Public REST + auth + submission outbox + schema owner |
| **execution-worker** | `execution-worker/` | Kafka consumer driving the per-submission pipeline |
| **sandbox-manager** | `sandbox-manager/` | Per-host privileged daemon; KVM + Firecracker + warm pool |
| **problem-service** | `problem-service/` | Problems + test_cases; V4-signed GCS URLs |
| **contest-service** | `contest-service/` | Contest lifecycle + system-test replay scheduler |
| **leaderboard-service** | `leaderboard-service/` | Verdict consumer + sharded Redis ZSET + STOMP fan-out |
| **scoring-pipeline** | `scoring-pipeline/` | Flink job for contest final scores (currently blocked on Flink cluster) |
| **analytics-pipeline** | `analytics-pipeline/` | Kafka → ClickHouse (currently not deployed) |

Two cross-cutting modules (not services, no owner page):

| Module | Directory | Role |
|---|---|---|
| **common** | `common/` | Shared proto definitions + region resolver |
| **in-guest agent** | `infra/firecracker/agent/` | Go binary that runs as PID 1 inside each microVM |

## What to read next — choose your path

| Goal | Read |
|---|---|
| Understand the architecture in depth | [`tech-spec.md`](./tech-spec.md) |
| Trace a submission end-to-end through code | [`flows/submission-roundtrip.md`](./flows/submission-roundtrip.md) |
| Modify one specific service | the matching [`services/<svc>.md`](./services/) |
| Understand why a particular design choice was made | [`adr/README.md`](./adr/README.md) |
| You're on-call and something is broken | the relevant [`services/<svc>.md`](./services/) §8 Runbook + [`runbooks/`](./runbooks/) |
| Plan a new feature touching multiple services | [`tech-spec.md`](./tech-spec.md) + [`design-docs/`](./design-docs/) |
| You hit an unfamiliar term | [`glossary.md`](./glossary.md) |

## Common gotchas a new reader hits

These are the issues that actually trip people up — drawn from the runbook sections of each service owner page.

- **"My execution-worker consumer-group lag is shown as `CURRENT-OFFSET = -`"** → the worker is crash-looping on bean wiring or schema validation. Check the worker logs. See [`services/execution-worker.md#81`](./services/execution-worker.md).
- **"My submission stays in `submissions` table forever, never gets a verdict"** → outbox row didn't get drained. Look for CRDB changefeed errors or check whether the reconciliation scanner is running. See [`flows/reconciliation-scanner.md`](./flows/reconciliation-scanner.md).
- **"Every submission returns WRONG_ANSWER, even trivial ones"** → canonical-hash drift. The agent's `stringTrim` and the worker's expected-hash computation must use the same rule. See [`services/execution-worker.md#83`](./services/execution-worker.md).
- **"Pool exhausted on the first submit after `docker compose up`"** → sandbox-manager hadn't finished warming the pool. Wait ~30 s and retry. See [`flows/pool-exhausted-backpressure.md`](./flows/pool-exhausted-backpressure.md).
- **"Why can't I `curl` from inside a microVM?"** → egress lockdown is intentional. The microVM netns has no external interfaces; vsock is the only path out. See [`design-docs/microvm-egress-lockdown.md`](./design-docs/microvm-egress-lockdown.md).
- **"My JWT keeps expiring mid-session"** → the SPA must call `POST /api/v1/auth/refresh` before the 15-min access JWT expires. The cookie-based refresh token covers this automatically if you're using the SPA. See [`flows/login-and-jwt-rotation.md`](./flows/login-and-jwt-rotation.md).

## Honesty about the dev experience

A few rough edges to set expectations:

- **macOS users get the Docker execution backend.** Security boundary is weaker (shared kernel) — fine for local dev, not used in production. Linux + KVM is the only path to a full-fidelity Firecracker run.
- **The compose file boots infra only.** Spring services run with `./gradlew :<svc>:bootRun`. There are open issues to add Dockerfiles + compose entries for the remaining services; tracked in `docs/services/README.md`.
- **Local dev is single-region.** You don't need the multi-region rollout for the dev loop. Production runs three regions; see [`design-docs/multi-region-rollout.md`](./design-docs/multi-region-rollout.md).
- **The smaller smoke is `print(2+2)`** at `infra/firecracker/test/problems/00000000-0000-0000-0000-0000000cafee/`. The realistic one is `sum-of-two`. Both work; pick the one that matches your scenario.
- **`scoring-pipeline` and `analytics-pipeline` are deferred.** No Flink cluster locally; no ClickHouse provisioned in production yet. The owner pages document the blockers.

## File map for the new reader

The top ~15 files you're most likely to grep for:

| Concern | File |
|---|---|
| Submission ingest controller | `api-gateway/src/main/java/com/onlinejudge/gateway/controller/SubmissionController.java` |
| Outbox + CRDB changefeed | `database/init.sql` + api-gateway `OutboxRelay`-class wiring |
| Reconciliation scanner | `api-gateway/.../reconciliation/StuckSubmissionScanner.java` |
| Kafka consumer (worker) | `execution-worker/.../consumer/SubmissionConsumer.java` |
| Idempotency state machine | `execution-worker/.../service/IdempotencyService.java` |
| Sandbox HTTP client | `execution-worker/.../service/SandboxManagerClient.java` |
| Firecracker execution path | `execution-worker/.../service/FirecrackerExecutionService.java` |
| Docker execution path (dev) | `execution-worker/.../service/DockerExecutionService.java` |
| Pool state machine | `sandbox-manager/.../pool/PoolManager.java` |
| Firecracker launcher | `sandbox-manager/.../firecracker/FirecrackerLauncher.java` |
| In-guest agent | `infra/firecracker/agent/cmd/agent/main.go` |
| vsock bridge | `infra/firecracker/vsock-client/main.go` |
| Leaderboard ZADD / STOMP | `leaderboard-service/.../consumer/VerdictConsumer.java` |
| SPA WebSocket subscription | `frontend/src/contexts/WebSocketContext.tsx` |
| Proto file | `common/src/main/proto/*.proto` |

For the full file-level map keyed by blog Part, see [`code-companion.md`](./code-companion.md).

## Where to go from here

If this took longer than 30 minutes, that's fine — the goal is the mental model, not the stopwatch. Open the doc that matches your next intent (see the "Choose your path" table above) and keep going.
