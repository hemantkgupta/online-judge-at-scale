# Glossary

> Last reconciled with the repo on 2026-05-19. Every term that appears more than twice across the docs and isn't obvious to a Principal SWE joining the team should be here. No definition longer than ~3 sentences — long-form belongs in `tech-spec.md` or the service owner pages.

## A

### ABS checkpointing
Asynchronous Barrier Snapshotting — Flink's mechanism for taking globally consistent state snapshots without stopping the pipeline. On failure, state restores from S3 + Kafka offsets rewind. Underpins exactly-once scoring in `scoring-pipeline`. See [`tech-spec.md#9-observability`](./tech-spec.md) and [`adr/0001-multi-service-event-driven.md`](./adr/0001-multi-service-event-driven.md).

### ACCEPTED
Canonical verdict status: the submission ran within its time/memory limits and every test case's stdout canonical-hash matched the expected.

### acks=all
Kafka producer setting: the broker only acks once all in-sync replicas have written. Used on every `evaluated_results` publish.

### AF_VSOCK
Linux socket family used to communicate between host and guest microVM. The OJ uses it as the only allowed channel into a sandboxed VM. See `oj-vsock-client`.

### AnalyticsEvent
Proto message published fire-and-forget on the `analytics` topic. Carries minimal submission metadata for the ClickHouse analytics pipeline. See [`tech-spec.md#5-wire-formats-and-data-models`](./tech-spec.md).

### Argon2id
Memory-hard password hashing algorithm; configured at ~64 MB / 3 iterations / 1 lane in api-gateway. The reason a `/auth/login` takes ~150 ms — deliberate.

### Artifact Registry
GCP's container image registry. SHA-tagged images live there; deploy pulls by SHA.

### at-least-once
Kafka's default delivery guarantee. Combined with idempotent consumers, becomes exactly-once at the application boundary.

### attempt cap
`app.idempotency.max-attempts` (default 5). When reached, the idempotency row is marked POISON and the submission is DLQ'd.

## B

### BlueStore
*(Not used in OJ; mentioned only via the parent CSE wiki.)*

### bootRun
Gradle target: `./gradlew :<svc>:bootRun` runs a Spring service locally without Docker. The dev loop on macOS where Firecracker isn't available.

### bounded out-of-orderness
Flink watermark strategy: `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(5))`. Lets the scorer absorb verdicts that arrive late by up to 5 min after the contest close.

## C

### canonical hash
SHA-256 of `stripTrailing(utf8(stdout))`. The trailing-whitespace strip ensures `4\n` and `4` compare equal across Python / Java / C++. The single most-likely source of false WRONG_ANSWER if implemented wrong.

### CDC (Change Data Capture)
Pattern of streaming a database's commit log as events. OJ uses CRDB native changefeeds for the outbox → Kafka publication.

### CE
COMPILE_ERROR — verdict when the contestant's code fails to compile.

### changefeed
CRDB feature: `CREATE CHANGEFEED FOR TABLE outbox INTO 'kafka://...'`. Emits one Kafka record per committed row insert.

### claim (idempotency)
The act of inserting/updating the `idempotency_keys` row to declare ownership of a submission's execution. Returns one of CLAIMED, IN_PROGRESS, COMPLETED, POISON.

### Cloud CDN
GCP CDN that fronts the React SPA. Cached at `/index.html` + bundled JS.

### Cloud DNS geo-routing
GCP DNS feature: route a single hostname to the nearest region by client IP. The OJ multi-region front door.

### CockroachDB (CRDB)
Distributed SQL database. Multi-region locality (REGIONAL BY ROW / GLOBAL), native changefeeds, Raft per range. See [`adr/0005-cockroachdb-over-postgres.md`](./adr/0005-cockroachdb-over-postgres.md).

### COMPLETED
Idempotency status: the submission's verdict was published and offset committed. Redeliveries see this and ack without re-execution.

### consumer group
Kafka primitive: a set of consumers that share a partition assignment. OJ uses `execution-worker-pretest`, `execution-worker-system`, `leaderboard`, `analytics`.

### consumer-group rebalance
Kafka re-assigns partitions when a consumer joins/leaves. Can trigger redelivery of unacked offsets.

### contestant
Authenticated user submitting code. Distinct from a "user" — a user becomes a contestant when they register for a contest.

### control-plane VM
The GCP VM hosting api-gateway, problem-service, contest-service, leaderboard-service. Distinct from the compute-plane VM(s) hosting execution-worker and sandbox-manager.

### CRC32c
Castagnoli polynomial CRC. Used in the OJ for chunk checksums when blobs land in storage layers — not heavily used in OJ proper.

## D

### DLQ envelope
JSON `{poisoned_at, last_error, submission_proto_b64}` published to `submissions.dlq` when a submission's idempotency row goes POISON. The operator can replay it manually.

### dead-letter queue (DLQ)
Topic `submissions.dlq`. Receives the envelopes for poisoned submissions.

### deploy time
~10 min from `tofu apply` + image push on a clean GCP project.

### destroy-never-reuse
SM invariant: every leased microVM is destroyed after `/release`, never recycled. Security boundary > throughput.

### dmClock
*(Not used in OJ; CSE-wiki term for QoS scheduler in distributed file systems.)*

### Docker (dev backend)
Fallback execution backend used when Firecracker is unavailable (macOS). `app.sandbox.backend=docker`.

### DUK
*(CSE-wiki term: Data Unique Key; not used in OJ proper.)*

## E

### egress lockdown
Per-microVM Linux netns with no interfaces except loopback + a vsock to the host. Contestant code cannot reach the public internet. See [`design-docs/microvm-egress-lockdown.md`](./design-docs/microvm-egress-lockdown.md).

### evaluated_results
Kafka topic for VerdictEvent. Consumed by leaderboard-service and scoring-pipeline.

### exactly-once
Application-level guarantee: every `(submission_id, phase)` produces exactly one verdict on `evaluated_results`. Achieved via idempotency keys + the worker's claim state machine.

### execution-worker
The Kafka consumer that drives the per-submission pipeline. See [`services/execution-worker.md`](./services/execution-worker.md).

## F

### family_id
Refresh-token rotation chain identifier. When token reuse is detected, the entire family is revoked. See [`flows/login-and-jwt-rotation.md`](./flows/login-and-jwt-rotation.md).

### Firecracker
AWS Lambda's microVM hypervisor. Used by OJ for hardware-isolated sandboxing. See [`adr/0004-firecracker-over-docker-for-prod.md`](./adr/0004-firecracker-over-docker-for-prod.md).

### Flink
Apache Flink. The scoring-pipeline runs on it. Managed state + ABS checkpointing.

### FNV-1a
*(CSE-wiki term; not used in OJ proper.)*

## G

### GCS
Google Cloud Storage. Holds test-case bundles. problem-service signs V4 URLs for the worker.

### GLOBAL table
CRDB locality: replicated to all regions, sub-10ms reads from anywhere. Used for `problems`, `contests`.

### gVisor
Userspace kernel that intercepts syscalls. Available as `runsc` for the Docker dev backend (`app.sandbox.docker.runtime=runsc`). Stronger isolation than `runc`, weaker than Firecracker.

## H

### HttpOnly cookie
Cookie attribute: not readable by JavaScript. Used for the refresh token to prevent XSS exfiltration.

## I

### IAP (Identity-Aware Proxy)
GCP service. Provides authenticated SSH tunnel into VMs for deploy. `gcloud compute ssh --tunnel-through-iap`.

### idempotency_keys
CRDB table owned (read+write) by execution-worker. One row per `(submission_id, phase)`. Drives the four-state claim machine.

### IN_PROGRESS
Idempotency status: a worker thread is currently processing this submission. Redeliveries nack 5 s.

### in-guest agent
PID 1 inside each microVM. A small Go binary that listens on vsock, compiles + runs the contestant's code per test case, returns per-test results.

### INTERNAL_ERROR
Verdict status: the system itself failed (lost vsock, watchdog kill mid-exec). Distinct from a contestant code failure.

### ISR (in-sync replica)
Kafka concept. With `acks=all` and `min.insync.replicas=2`, every publish needs 2 in-sync replicas to ack.

## J

### jailer
Firecracker's chroot/cgroup setup utility. Run before the Firecracker binary itself.

### JWT (access)
JSON Web Token. 15-min expiry. HMAC-SHA256 signed. Sent in `Authorization: Bearer`. Self-contained — no DB lookup on the hot path.

## K

### KRaft
Kafka's Zookeeper-less mode. The 3-broker production deployment uses KRaft. See [`design-docs/kafka-cluster-and-crdb-cluster.md`](./design-docs/kafka-cluster-and-crdb-cluster.md).

### KVM
Linux Kernel-based Virtual Machine. Firecracker needs `/dev/kvm`. macOS doesn't have it — hence the Docker fallback.

## L

### leaderboard:{contestId}:s{idx}
Redis key for one score-range shard of a contest's leaderboard. `s0` is the highest-scoring shard, increasing by index. See [`adr/0008-score-range-sharded-leaderboard-zset.md`](./adr/0008-score-range-sharded-leaderboard-zset.md).

### lease
The act of taking a microVM from the SM warm pool for the duration of one submission. POSTed via `/lease`; released via `/release` or the watchdog.

### libcephfs
*(CSE-wiki term; not used in OJ proper.)*

### lockdown
Short for "egress lockdown" — the per-microVM network namespace setup.

## M

### max.poll.records
Kafka consumer setting. Set to 1 in execution-worker — one submission in flight per thread, natural backpressure.

### MirrorMaker 2
Kafka tool for cross-cluster replication. Rejected for the OJ multi-region design. See [`adr/0007-per-region-kafka-no-cross-region-mirror.md`](./adr/0007-per-region-kafka-no-cross-region-mirror.md).

### MicroVM
Lightweight virtual machine from Firecracker. ~5-10 MB overhead, ~125 ms boot. The unit of contestant code isolation.

### MLE
MEMORY_LIMIT_EXCEEDED — verdict status.

### mTLS
Mutual TLS. Both sides authenticate via certificates. Used for CRDB inter-node and (planned) inter-service communication.

## O

### oj-vsock-client
The ~250-LOC Go binary that bridges the JVM-based execution-worker to vsock. Bundled into the worker container image. See [`adr/0006-vsock-go-bridge-not-jni.md`](./adr/0006-vsock-go-bridge-not-jni.md).

### ordinal
Per-test-case integer index (0..N-1) within a problem. Used in canonical hash + per_test breakdown.

### OTel (OpenTelemetry)
Cross-vendor observability instrumentation. The execution-worker's metrics, traces, and logs go through OTel SDK → OTLP → Collector → GCP backends. See [`adr/0009-otel-otlp-over-prometheus-pull.md`](./adr/0009-otel-otlp-over-prometheus-pull.md).

### outbox
Pattern: `INSERT submissions ... ; INSERT outbox ... ;` in one CRDB tx. CRDB changefeed reads `outbox` rows and publishes to Kafka. See [`adr/0002-transactional-outbox-over-sync-publish.md`](./adr/0002-transactional-outbox-over-sync-publish.md).

## P

### partition
Kafka topic subdivision. The OJ keys by `user_id` so per-user submission ordering is preserved within a partition.

### per_test breakdown
The list of per-ordinal verdicts inside a VerdictEvent. Lets the SPA show "ACCEPTED on tests 1-3, WRONG_ANSWER on 4".

### Phase 1
The pretest phase. Runs during the contest. Public test cases only.

### Phase 2
The system-test phase. Runs after contest close. Full hidden test suite. Final verdicts.

### Phase 1 → Phase 2 promotion
After a Phase-1 ACCEPTED, the worker re-publishes the original SubmissionEvent to `submissions.system` with phase=system. See [`flows/contest-close-and-system-tests.md`](./flows/contest-close-and-system-tests.md).

### PID 1
The init process inside a microVM. The OJ runs the in-guest agent directly as PID 1; no shell, no systemd.

### POISON
Idempotency status: the row has exceeded `max-attempts`. The DLQ collects it; the operator inspects.

### pool_exhausted
SM's 503 response body when the warm pool has no READY VMs for the requested language. Includes `retry_after_ms`. See [`flows/pool-exhausted-backpressure.md`](./flows/pool-exhausted-backpressure.md).

### pretest
The public subset of test cases. Used in Phase 1 for fast feedback.

### problem-service
Owns `problems` + `test_cases`. Signs V4 GCS download URLs. See [`services/problem-service.md`](./services/problem-service.md).

## R

### RBR (REGIONAL BY ROW)
CRDB locality: each row is pinned to a region based on a column value. Used for `submissions` so writes don't cross regions.

### Raft group
CRDB's per-range consensus group. One node failure tolerated transparently.

### reconciliation scanner
api-gateway's periodic sweep for stuck `submissions` rows. Re-publishes them to Kafka. See [`flows/reconciliation-scanner.md`](./flows/reconciliation-scanner.md).

### refresh token
32 random bytes in an HttpOnly cookie. Server stores SHA-256(raw). Rotated on every refresh. Family revoked on detected reuse.

### REGISTRATION
Contest state: open for signups, not yet ACTIVE.

### release
SM `/release` endpoint. Destroys the leased microVM.

### retry_after_ms
Field in SM's pool_exhausted 503 body. The worker uses it as the nack duration.

### rootfs
Per-language read-only filesystem image baked into each Firecracker microVM. Contains the language runtime + `oj-agent` binary.

### runc / runsc
Container runtimes. `runc` is the default; `runsc` (gVisor) is a stronger sandbox. Configured per `app.sandbox.docker.runtime`.

### RE
RUNTIME_ERROR — verdict status.

## S

### sandbox-manager (SM)
The per-host privileged daemon that owns the microVM pool, KVM access, and the `/lease`/`/release` API. See [`services/sandbox-manager.md`](./services/sandbox-manager.md).

### sandboxId
SM's identifier for one leased microVM. Returned from `/lease`, passed back to `/release`.

### score-range sharded ZSET
Redis sorted-set sharding strategy: `leaderboard:{contestId}:s{idx}`. Avoids the hot-key collapse of a single ZSET per contest.

### seccomp
Linux syscall filtering. The OJ's Docker backend can apply a seccomp profile at `/etc/seccomp/sandbox-seccomp.json`.

### session_token
Field in the agent dispatch request. A random short-lived token the agent uses to authenticate that this dispatch came from the worker (defense-in-depth inside the vsock channel).

### signed URL (V4)
GCS V4-signed URL with a 5-min TTL. problem-service signs them for the worker's GET against the test-case bucket.

### STOMP
Simple Text-Oriented Messaging Protocol. The verdict push from leaderboard-service to the SPA rides STOMP-over-SockJS.

### SubmissionEvent
Proto message published to `submissions.pretest` (or `submissions.system` for Phase 2). Contains the contestant's submission payload.

### submissions.dlq
Kafka topic for DLQ envelopes.

### submissions.pretest
Kafka topic for Phase-1 SubmissionEvents.

### submissions.system
Kafka topic for Phase-2 SubmissionEvents.

### sum-of-two
The reference smoke-test problem. 5 test cases. Reference solutions in Python / Java / C++. Lives in `infra/firecracker/test/problems/sum-of-two/`.

### SYSTEM_TESTING
Contest state: between CLOSED and FINALIZED. Phase 2 verdicts are being processed.

## T

### tap interface
Per-microVM Linux network interface. Created in a fresh netns with no routes to the outside.

### Tectonic
*(CSE-wiki term; not used in OJ proper.)*

### tech-spec.md
The canonical architecture reference. 16 sections + 4 appendices.

### TLE
TIME_LIMIT_EXCEEDED — verdict status.

### token bucket
Generic rate-limiter algorithm. Used by api-gateway for `/auth/*` and `/api/v1/submissions/*` limiters (separate buckets).

### topic
Kafka primitive. The OJ uses: `submissions.pretest`, `submissions.system`, `submissions.dlq`, `evaluated_results`, `analytics`, `contest.events`, `contest.final_scores`.

## U

### UDS (Unix Domain Socket)
Path-named socket in the filesystem. Per-microVM vsock surfaces as `/tmp/fc-<uuid>-vsock.sock` on the host.

## V

### V4 signed URL
GCS signed URL format. The OJ uses 5-min TTLs.

### VerdictEvent
Proto message published to `evaluated_results` after a submission completes. Contains the overall verdict + per_test breakdown.

### virtual queue
Per-tenant slot in a multi-tenant resource. *(More relevant in CSE wiki; OJ uses per-language pools rather than per-tenant.)*

### vsock
The OS socket family used between host and microVM guest. The OJ's only allowed path into a sandboxed VM.

### vsock UDS
The host-side filesystem entry for the vsock channel: `/tmp/fc-<uuid>-vsock.sock`.

## W

### WAL (write-ahead log)
*(Generic distributed-systems concept; not directly used in OJ proper. CRDB has one internally.)*

### watchdog
SM's per-VM timer. After `app.lease.wall-seconds` (default 30), the VM is force-killed regardless of state.

### WIF (Workload Identity Federation)
GCP feature for federating non-GCP credentials. GitHub Actions uses WIF to deploy without storing service-account JSON.

### WRONG_ANSWER
Verdict status: code ran successfully but stdout's canonical hash did not match expected.

## Z

### ZippyDB
*(CSE-wiki term — Meta's KV store; not used in OJ proper.)*

### Zookeeper
The pre-KRaft Kafka coordination service. OJ used it briefly; production now uses KRaft.

### ZSET (Redis sorted set)
Redis data structure used for the leaderboard. `ZADD key score member`. Range queries via `ZRANGE` / `ZRANGEBYSCORE`.
