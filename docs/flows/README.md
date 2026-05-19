# Cross-Service Flows

> Last reconciled with the repo on 2026-05-19. If a flow disagrees with code, treat the code as authoritative and file a ticket.

End-to-end sequence walkthroughs that cross multiple services. Each flow names every service it touches, the invariants preserved at each step, and the failure modes at each step.

The per-service detail lives in [`../services/`](../services/). The cross-cutting architectural reference is [`../tech-spec.md`](../tech-spec.md). Forward-looking specs are in [`../design-docs/`](../design-docs/).

## All flows

| Flow | Role | Read this when… |
|---|---|---|
| [`submission-roundtrip.md`](./submission-roundtrip.md) | The canonical happy path: SPA → gateway → outbox → Kafka → worker → SM → microVM → verdict → leaderboard → SPA push. The system's defining workload. | You want a single document that traces every hop a submission takes. Read this first. |
| [`login-and-jwt-rotation.md`](./login-and-jwt-rotation.md) | Argon2id signup, login, short-lived access JWT + opaque refresh token, refresh-token rotation with family revocation on reuse detection. | You're debugging auth, designing a new endpoint, or wondering "what happens if a refresh token leaks?" |
| [`pool-replenishment.md`](./pool-replenishment.md) | Sandbox warm-pool state machine: spawner → Firecracker boot → agent ready → READY → LEASED → DESTROYED. The mechanism that hides ~125 ms VM boot from `/lease`. | You're tuning `app.pool.targets.*`, debugging slow boots, or wondering why VMs are never recycled. |
| [`pool-exhausted-backpressure.md`](./pool-exhausted-backpressure.md) | What happens when SM returns 503 pool_exhausted: PoolExhaustedException → ack.nack(retry_after_ms) → no idempotency mutation → eventual lease success. | You're seeing `oj.worker.pool_exhausted_total` climb or contestants reporting unexpected RUNTIME_ERROR. |
| [`kafka-redelivery-and-idempotency.md`](./kafka-redelivery-and-idempotency.md) | The four branches at `IdempotencyService.claimSubmission`: CLAIMED / IN_PROGRESS / COMPLETED / POISON. Why "claim AFTER lease, not before." | You're touching the idempotency code, investigating a stuck submission, or trying to understand the DLQ. |
| [`reconciliation-scanner.md`](./reconciliation-scanner.md) | The api-gateway-side scanner that catches submissions whose path stalled (outbox not drained, worker died mid-execution) and re-drives them safely. | You're investigating `submissions` rows that never got a verdict, or planning to widen what gets reconciled. |
| [`contest-close-and-system-tests.md`](./contest-close-and-system-tests.md) | Contest state machine: ACTIVE → CLOSED → SYSTEM_TESTING → FINALIZED, batched Phase-2 replay onto `submissions.system`, Flink-driven final scores. | You're running a contest, debugging final-score computation, or designing the next phase model. |

## How a flow page is structured

Every page follows the same shape:

1. **Why this flow exists** — the architectural problem it solves.
2. **Sequence** — a Mermaid diagram or prose+bullets when a diagram would feel forced.
3. **Step-by-step walkthrough** — numbered steps with file:line pointers and the invariant each step preserves.
4. **Failure modes at each step** — a table: step → failure → detection → behaviour.
5. **Related material** — back-links to `services/*`, `tech-spec.md` sections, `design-docs/`, and ADRs.

When adding a new flow, copy this structure. When updating an existing flow, keep the structure stable — readers depend on knowing where to look.

## What flows are deliberately NOT documented here

- **Single-service flows** (e.g. "leaderboard score update") — those belong in the service's owner page under §3 Internal design.
- **Operational procedures** (deploy, rollback, scale-up) — those belong in `runbooks/` and per-service owner pages §8 Runbook.
- **Forward-looking designs not yet built** — those belong in `design-docs/`.
