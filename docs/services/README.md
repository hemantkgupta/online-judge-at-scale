# Service owner pages

Per-service deep-dive references. Each page is the authoritative spec for one service: purpose, external interfaces, internal design, data ownership, failure modes, configuration reference, metrics emitted, runbook, tests, code map.

For the cross-cutting view — architecture diagrams, wire-format contracts (proto, Kafka, CRDB, Redis, GCS layout), the auth model in spec form, multi-region story — see [`../tech-spec.md`](../tech-spec.md).

## All services

Every service has its own owner page. The cross-cutting modules `common` (shared protobuf + region resolver) and the in-guest Go agent (PID 1 inside each microVM) are described inline in tech-spec §4.9 and §4.10 because they don't fit the "owned by one team" model.

| Service | Page | Role | Deployment status | Words |
|---|---|---|---|---|
| **api-gateway** | [`api-gateway.md`](./api-gateway.md) | Public-facing HTTP API, contestant identity (signup / login / JWT rotation), submission acceptance with outbox pattern, reconciliation scanner, schema owner via Flyway | Live | 3.6K |
| **execution-worker** | [`execution-worker.md`](./execution-worker.md) | Kafka consumer that drives the per-submission pipeline: source fetch → test-case fetch → sandbox lease → agent dispatch → verdict publish. Idempotency + DLQ live here | Live | 3.9K |
| **sandbox-manager** | [`sandbox-manager.md`](./sandbox-manager.md) | Per-host privileged daemon. Pool state machine, watchdog, cgroups, network namespace egress lockdown, Firecracker lifecycle | Live | 4.2K |
| **problem-service** | [`problem-service.md`](./problem-service.md) | Reads `problems` + `test_cases`; signs V4 GCS download URLs (5-min TTL) for the worker | Live | 2.8K |
| **contest-service** | [`contest-service.md`](./contest-service.md) | Contest lifecycle state machine (CREATED → REGISTRATION → ACTIVE → CLOSED), system-test replay on close | Image shipped, awaits deploy | 3.3K |
| **leaderboard-service** | [`leaderboard-service.md`](./leaderboard-service.md) | Kafka consumer for verdicts + Redis sharded sorted-set + STOMP-over-SockJS WebSocket fan-out | Image shipped, awaits deploy | 2.8K |
| **scoring-pipeline** | [`scoring-pipeline.md`](./scoring-pipeline.md) | Flink job that would compute contest scores from verdicts; writes to Redis leaderboard | **BLOCKED** — Flink cluster required | 2.1K |
| **analytics-pipeline** | [`analytics-pipeline.md`](./analytics-pipeline.md) | Kafka consumer for `analytics_events` → ClickHouse | **NOT DEPLOYED** — no Dockerfile, no ClickHouse provisioned | 1.4K |

Total: ~24K words of owner-spec content across 8 pages.

## Page structure

Every owner page follows the same 11-section template so a reader knows exactly where to look:

1. **Purpose** — what this service owns; what it doesn't.
2. **External interfaces** — REST endpoints, Kafka topics consumed/produced, outbound HTTP, listening surface.
3. **Internal design** — key mechanisms specific to this service.
4. **Data ownership** — which tables/topics/keys this service writes vs reads. Cross-cutting Redis / Kafka contracts referenced.
5. **Failure modes** — concrete failure-to-detection-to-behaviour table.
6. **Configuration reference** — every property + default + purpose.
7. **Metrics emitted** — name + type + labels + meaning. May be "proposed, not yet implemented" for services that haven't shipped a metrics bean.
8. **Runbook** — common incidents with diagnose + fix.
9. **Tests & verification** — unit + integration + manual smoke.
10. **Relevant design docs** — pointers to `../design-docs/` for forward-looking design context.
11. **Code map** — concern → file mapping.

When adding a new service to the system, copy this structure into a new page. When updating an existing page, keep the structure stable — readers depend on knowing where to find each kind of information.

## When the tech-spec and an owner page disagree

The owner page is authoritative for **implementation details** (which class does what, which Redis key is actually used, what shape a method has). The tech-spec is authoritative for **cross-cutting contracts** (the proto schema, the Kafka topic catalogue, the auth model — anything that multiple services must agree on). If the tech-spec describes an implementation detail that no longer matches a service, update the tech-spec to point at the owner page; the page is the source of truth for the implementation. If an owner page contradicts a cross-cutting contract, fix the page — the service is buggy.

This split surfaced concretely during the first round of owner-page authoring: tech-spec §5.4 documented `verdict-cache:{submissionId}` TTL 1h but the leaderboard-service code actually used `verdict:{submissionId}` TTL 24h. The fix was to update tech-spec §5.4 to match the code (the implementation was correct; the spec had drifted). Similar fixes: `analytics` topic → `analytics_events`, single-key ZSET → score-range-sharded `leaderboard:{contestId}:s{idx}`, WebSocket endpoint path corrections.
