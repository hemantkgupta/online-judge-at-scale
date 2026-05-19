# Architecture Decision Records

> Each ADR captures one significant design decision: the context that forced it, what was chosen, what was rejected, and what each choice commits us to.

## All ADRs

| ID | Title | Status | Date | One-line summary |
|---|---|---|---|---|
| [0001](./0001-multi-service-event-driven.md) | Multi-Service Event-Driven Architecture with Kafka Spine | Accepted | 2026-04-24 | 10-service architecture on a Kafka spine; Firecracker, CRDB, Flink. The umbrella decision. |
| [0002](./0002-transactional-outbox-over-sync-publish.md) | Transactional outbox over sync Kafka publish | Accepted | 2026-05-19 | Same-transaction outbox row + CRDB changefeed; no submission lost to broker hiccups. |
| [0003](./0003-idempotency-claim-after-lease.md) | Claim idempotency AFTER lease, not before | Accepted | 2026-05-19 | Pool-exhausted retries don't consume idempotency budget. Bug-history driven. |
| [0004](./0004-firecracker-over-docker-for-prod.md) | Firecracker over Docker in production | Accepted | 2026-05-19 | Hardware-isolated sandboxing for adversarial code. Docker = dev fallback only. |
| [0005](./0005-cockroachdb-over-postgres.md) | CockroachDB over PostgreSQL | Accepted | 2026-05-19 | Two locality policies (RBR + GLOBAL) on one cluster; native changefeeds. |
| [0006](./0006-vsock-go-bridge-not-jni.md) | vsock Go bridge, not JNI | Accepted | 2026-05-19 | ~250-line Go binary owns vsock; JVM stays libc-free. |
| [0007](./0007-per-region-kafka-no-cross-region-mirror.md) | Per-region Kafka, no cross-region mirror | Accepted | 2026-05-19 | Each region's data plane is self-contained; aggregation happens in CRDB / Flink. |
| [0008](./0008-score-range-sharded-leaderboard-zset.md) | Score-range sharded leaderboard ZSET | Accepted | 2026-05-19 | `leaderboard:{contestId}:s{idx}` shards avoid the single-ZSET hot-key collapse. |
| [0009](./0009-otel-otlp-over-prometheus-pull.md) | OTel OTLP over Prometheus pull | Accepted | 2026-05-19 | Push-based observability handles short-lived microVMs; per-region Collector decouples backend. |
| [0010](./0010-eleven-section-owner-page-template.md) | 11-section service owner-page template | Accepted | 2026-05-19 | Docs-process decision: predictable structure on every service page. |

## When to write a new ADR

Write one when you're about to:

- Commit to an operationally heavyweight component (a new datastore, a new compute substrate).
- Reject an obvious choice and the rejection rationale will not be remembered six months from now.
- Make a security-boundary decision (sandbox shape, authn model, secrets handling).
- Change a contract that multiple services depend on (proto schema, topic naming, table locality).
- Reverse a previous decision (mark the old ADR `Superseded by NNN`).

Don't write one for:

- Single-file refactors.
- Naming choices.
- Dev-loop preferences.
- Anything where the rationale is "the alternative was clearly worse" with no surprising tradeoffs.

## ADR shape

Every ADR follows the same template:

```markdown
# ADR-NNNN: <Decision name>

**Status**: Accepted | Superseded by NNNN
**Date**: YYYY-MM-DD
**Deciders**: ...

## Context
The forcing function. What's the problem? What constraints make the obvious answer wrong?

## Decision
The thing chosen, named precisely.

## Alternatives considered
Each with a 2-3 sentence "why rejected".

## Consequences
**Positive:** what this commits us to that is good.
**Negative:** what gets harder.

## Implementation pointers
File paths where this lives in code.

## Related
Wikilinks to tech-spec sections, services pages, design-docs, other ADRs.
```

When superseding an older ADR, keep the older file but mark its Status. Cross-link both directions.

## Decisions worth ADRs but not yet captured

The following decisions exist in code but aren't formalised as ADRs. Each is a candidate for a future entry:

- **Argon2id + SHA-256-hashed refresh tokens with forced rotation.** The "refresh tokens are 32 raw bytes, server stores SHA-256(raw), rotation revokes old on every refresh" design is non-trivial. See [`services/api-gateway.md`](../services/api-gateway.md) §3.1.
- **api-gateway owns the schema for all services via single-txn Flyway.** Centralising migrations versus per-service ownership is a load-bearing org/process choice. See [`services/api-gateway.md`](../services/api-gateway.md) §3.5.
- **`auto-offset-reset: latest` on the leaderboard verdict consumer.** Missed verdicts are not replayed; HTTP fallback covers. See [`services/leaderboard-service.md`](../services/leaderboard-service.md) §2.1.
- **Destroy-never-reuse sandbox invariant.** Every lease destroys the VM; never recycled. Security > throughput. See [`services/sandbox-manager.md`](../services/sandbox-manager.md) §1.
- **`max.poll.records=1` per worker thread.** Backpressure-by-not-polling instead of batch-and-throttle. See [`services/execution-worker.md`](../services/execution-worker.md) §2.1.
