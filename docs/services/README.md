# Service owner pages

Per-service deep-dive references. Each page is the authoritative spec for one service: purpose, external interfaces, internal design, data ownership, failure modes, configuration reference, metrics emitted, runbook, tests, code map.

For the cross-cutting view — architecture diagrams, wire-format contracts (proto, Kafka, CRDB, Redis, GCS layout), the auth model in spec form, multi-region story — see [`../tech-spec.md`](../tech-spec.md).

## High-complexity services (dedicated owner pages)

| Service | Page | Role | Approx. words |
|---|---|---|---|
| **api-gateway** | [`api-gateway.md`](./api-gateway.md) | Public-facing HTTP API, contestant identity (signup / login / JWT rotation), submission acceptance with outbox pattern, reconciliation scanner, schema owner via Flyway | 3.6K |
| **execution-worker** | [`execution-worker.md`](./execution-worker.md) | Kafka consumer that drives the per-submission pipeline: source fetch → test-case fetch → sandbox lease → agent dispatch → verdict publish. Idempotency + DLQ live here | 3.9K |
| **sandbox-manager** | [`sandbox-manager.md`](./sandbox-manager.md) | Per-host privileged daemon. Pool state machine, watchdog, cgroups, network namespace egress lockdown, Firecracker lifecycle | 4.2K |
| **problem-service** | [`problem-service.md`](./problem-service.md) | Reads `problems` + `test_cases`; signs V4 GCS download URLs (5-min TTL) for the worker | 2.8K |

## Thinner services (covered inline in tech-spec §4)

These don't have dedicated pages today because their complexity fits inside the tech-spec component-catalogue subsection. If any grows in scope (especially `contest-service` once contest-mode admin endpoints land, or `scoring-pipeline` once Flink is deployed), it should graduate to its own owner page following the same 11-section template these four use.

- **contest-service** — tech-spec §4.5. Contest lifecycle state machine + system-test replay on close.
- **leaderboard-service** — tech-spec §4.6. Kafka consumer + Redis sorted-set + WebSocket fan-out.
- **scoring-pipeline** — tech-spec §4.7. **BLOCKED**: requires an external Flink cluster.
- **analytics-pipeline** — tech-spec §4.8. Stub today; consumes the `analytics` topic.
- **common** — tech-spec §4.9. Shared protobuf + region resolver. No runtime surface.
- **In-guest agent (Go)** — tech-spec §4.10. PID 1 inside each microVM. Listens on vsock, runs contestant code, returns per-test verdicts.

## Page structure

Every owner page follows the same 11-section template so a reader knows exactly where to look:

1. **Purpose** — what this service owns; what it doesn't.
2. **External interfaces** — REST endpoints, Kafka topics consumed/produced, outbound HTTP, listening surface.
3. **Internal design** — key mechanisms specific to this service.
4. **Data ownership** — which tables/topics/keys this service writes vs reads. Cross-cutting Redis / Kafka contracts referenced.
5. **Failure modes** — concrete failure-to-detection-to-behaviour table.
6. **Configuration reference** — every property + default + purpose.
7. **Metrics emitted** — name + type + labels + meaning.
8. **Runbook** — common incidents with diagnose + fix.
9. **Tests & verification** — unit + integration + manual smoke.
10. **Relevant design docs** — pointers to `../design-docs/` for forward-looking design context.
11. **Code map** — concern → file mapping.

When adding a new service page, copy this structure. When updating an existing page, keep the structure stable — readers depend on knowing where to find each kind of information.
