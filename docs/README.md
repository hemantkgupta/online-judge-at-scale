# Online Judge — documentation hub

> **Canonical narrative:** the blog at `raw-blog/online-judge-at-scale.md` (in the parent CSE-Raw repo). The blog explains the architecture from first principles. This folder describes what is *actually built* and how the code lines up against the design.
>
> Last reconciled with the repo on 2026-05-19.

If you're new, start by skimming this page to pick your entry point. Then come back to the path that matches what you're trying to do.

## Pick your path

| If you want to … | Start with | Then |
|---|---|---|
| Understand what this repo IS in 5 minutes | [`getting-started.md`](./getting-started.md) → [`tech-spec.md` §1](./tech-spec.md) | the blog post |
| Run the code locally and make your first submission | root [`README.md`](../README.md) → [`getting-started.md`](./getting-started.md) | [`flows/submission-roundtrip.md`](./flows/submission-roundtrip.md) |
| Trace one submission end-to-end through the code | [`flows/submission-roundtrip.md`](./flows/submission-roundtrip.md) | per-service: [`services/api-gateway.md`](./services/api-gateway.md) → [`services/execution-worker.md`](./services/execution-worker.md) → [`services/sandbox-manager.md`](./services/sandbox-manager.md) |
| Modify a specific service | [`services/<svc>.md`](./services/) | the [`flows/`](./flows/) that involve it |
| Understand a cross-service flow (auth, contest close, recovery) | [`flows/README.md`](./flows/README.md) | the relevant [`services/*`](./services/) |
| Understand why a particular design choice was made | [`adr/README.md`](./adr/README.md) | the relevant ADR |
| Operate the system (on-call, recovery) | [`services/<svc>.md`](./services/) Runbook sections | [`runbooks/`](./runbooks/) |
| Plan a new feature touching multiple services | [`tech-spec.md`](./tech-spec.md) → [`design-docs/`](./design-docs/) | [`adr/`](./adr/) (if your decision is significant) |
| Look up an unfamiliar term | [`glossary.md`](./glossary.md) | [`tech-spec.md#15-glossary`](./tech-spec.md) |
| Find the file that implements something the blog mentions | [`code-companion.md`](./code-companion.md) | the per-service code map (§11) on each owner page |

## The doc tree

### Reference

- [`tech-spec.md`](./tech-spec.md) — canonical architecture reference. 16 sections + 4 appendices. Read this when you need a cross-cutting view that no single service page can give you.
- [`glossary.md`](./glossary.md) — vocabulary lookup. 120+ entries. Any term you hit twice in another doc should be here.
- [`code-companion.md`](./code-companion.md) — file-level map from each blog Part → the Java source files that implement it. Single most useful doc when reading the blog with the codebase open.
- [`ci-cd.md`](./ci-cd.md) — CI/CD architecture (GitHub Actions + Workload Identity Federation + IAP-tunneled deploy).
- [`getting-started.md`](./getting-started.md) — the "first 30 minutes with this repo" walkthrough.

### Per-service owner pages — [`services/`](./services/)

Eight authoritative specs, one per service. Each follows a strict 11-section template (Purpose / External interfaces / Internal design / Data ownership / Failure modes / Configuration / Metrics / Runbook / Tests / Design docs / Code map).

| Service | Page | Status |
|---|---|---|
| api-gateway | [`services/api-gateway.md`](./services/api-gateway.md) | Live |
| execution-worker | [`services/execution-worker.md`](./services/execution-worker.md) | Live |
| sandbox-manager | [`services/sandbox-manager.md`](./services/sandbox-manager.md) | Live |
| problem-service | [`services/problem-service.md`](./services/problem-service.md) | Live |
| contest-service | [`services/contest-service.md`](./services/contest-service.md) | Image shipped |
| leaderboard-service | [`services/leaderboard-service.md`](./services/leaderboard-service.md) | Image shipped |
| scoring-pipeline | [`services/scoring-pipeline.md`](./services/scoring-pipeline.md) | Blocked (Flink) |
| analytics-pipeline | [`services/analytics-pipeline.md`](./services/analytics-pipeline.md) | Not deployed |

See [`services/README.md`](./services/README.md) for the full index + template description.

### Cross-service flows — [`flows/`](./flows/)

End-to-end sequence walkthroughs that cross multiple services. Each one has a Mermaid sequence diagram + numbered step walkthrough + failure modes table.

| Flow | Role |
|---|---|
| [`submission-roundtrip.md`](./flows/submission-roundtrip.md) | The canonical happy path |
| [`login-and-jwt-rotation.md`](./flows/login-and-jwt-rotation.md) | Argon2id signup + JWT + refresh-token rotation |
| [`pool-replenishment.md`](./flows/pool-replenishment.md) | SM warm-pool state machine |
| [`pool-exhausted-backpressure.md`](./flows/pool-exhausted-backpressure.md) | 503 pool_exhausted → ack.nack → retry |
| [`kafka-redelivery-and-idempotency.md`](./flows/kafka-redelivery-and-idempotency.md) | The 4 branches of the idempotency claim |
| [`reconciliation-scanner.md`](./flows/reconciliation-scanner.md) | The stuck-submission safety net |
| [`contest-close-and-system-tests.md`](./flows/contest-close-and-system-tests.md) | Phase 2 replay → final scores → FINALIZED |

See [`flows/README.md`](./flows/README.md) for the flow-page template.

### Architecture decisions — [`adr/`](./adr/)

Compact records of significant design decisions: context, what was chosen, what was rejected, what each choice commits us to.

| ADR | Title |
|---|---|
| [0001](./adr/0001-multi-service-event-driven.md) | Multi-Service Event-Driven Architecture with Kafka Spine |
| [0002](./adr/0002-transactional-outbox-over-sync-publish.md) | Transactional outbox over sync Kafka publish |
| [0003](./adr/0003-idempotency-claim-after-lease.md) | Claim idempotency AFTER lease, not before |
| [0004](./adr/0004-firecracker-over-docker-for-prod.md) | Firecracker over Docker in production |
| [0005](./adr/0005-cockroachdb-over-postgres.md) | CockroachDB over PostgreSQL |
| [0006](./adr/0006-vsock-go-bridge-not-jni.md) | vsock Go bridge, not JNI |
| [0007](./adr/0007-per-region-kafka-no-cross-region-mirror.md) | Per-region Kafka, no cross-region mirror |
| [0008](./adr/0008-score-range-sharded-leaderboard-zset.md) | Score-range sharded leaderboard ZSET |
| [0009](./adr/0009-otel-otlp-over-prometheus-pull.md) | OTel OTLP over Prometheus pull |
| [0010](./adr/0010-eleven-section-owner-page-template.md) | 11-section service owner-page template |

See [`adr/README.md`](./adr/README.md) for the ADR process and the full index.

### Forward-looking designs — [`design-docs/`](./design-docs/)

Standalone specs for the heavyweight items on the production-readiness roadmap. Each doc is concrete enough that an engineer can implement directly.

| Doc | Topic |
|---|---|
| [`auth-end-to-end.md`](./design-docs/auth-end-to-end.md) | Full auth design (Argon2id + JWT + refresh rotation + audit) |
| [`otel-collector-deployment.md`](./design-docs/otel-collector-deployment.md) | OTel Collector deployment + dashboards |
| [`kafka-cluster-and-crdb-cluster.md`](./design-docs/kafka-cluster-and-crdb-cluster.md) | 3-broker KRaft Kafka + 3-node CRDB with mTLS |
| [`microvm-egress-lockdown.md`](./design-docs/microvm-egress-lockdown.md) | Per-microVM netns + host iptables UID-DROP rules |
| [`ci-cd-github-actions.md`](./design-docs/ci-cd-github-actions.md) | GHA workflows + WIF + branch protection |
| [`react-spa-and-websockets.md`](./design-docs/react-spa-and-websockets.md) | Vite + monaco-editor + STOMP-over-SockJS |
| [`contest-close-system-tests-replay.md`](./design-docs/contest-close-system-tests-replay.md) | Contest-close lifecycle + system-test replay |
| [`multi-region-rollout.md`](./design-docs/multi-region-rollout.md) | 3-region rollout (asia-south1, us-east1, europe-west1) |

See [`design-docs/README.md`](./design-docs/README.md) for the index + decisions deliberately NOT given a design doc.

### Runbooks — [`runbooks/`](./runbooks/)

Operational procedures. Per-service runbook sections live in each `services/<svc>.md#8` — the `runbooks/` folder is for cross-cutting operations.

- [`multi-region.md`](./runbooks/multi-region.md)

### History / planning

- [`implementation-plan.md`](./implementation-plan.md) — the original phase-by-phase plan; largely executed.
- [`parity-plan.md`](./parity-plan.md) — the historical plan that brought the repo to parity with a sister project.
- [`research-checkpoint.md`](./research-checkpoint.md) — design rationale for foundational mechanisms.

## How docs and code stay in sync

The **sync rule** (declared in [`code-companion.md`](./code-companion.md)): *if the blog claims a mechanism exists, the companion must point to the file or test that implements it.* If no file exists, the entry says so, and the **Gaps** section names what's intentionally not implemented locally.

Two failure modes the sync rule protects against:

1. Blog claims a feature that doesn't exist in code (false advertising).
2. Code has a feature the blog never mentions (orphaned implementation).

Both `code-companion.md` and the blog's gap table get updated together when the code changes.

The same rule applies to the docs in this folder. When updating a service's behaviour, update the matching `services/<svc>.md`, `flows/*` that touch it, the `adr/` if you've made a new decision, and `tech-spec.md` if you've changed a cross-cutting contract.

## Running the code

See the root [`README.md`](../README.md) — `docker compose up -d` + `./gradlew test` gets you from clone to a green test suite. The [`getting-started.md`](./getting-started.md) walkthrough explains what you're seeing.
