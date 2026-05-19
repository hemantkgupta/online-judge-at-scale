# ADR-0002: Transactional Outbox over Synchronous Kafka Publish

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

api-gateway's `POST /api/v1/submissions` returns 202 Accepted, but the contract is stronger than "we received it": the submission must subsequently appear on `submissions.pretest` exactly once, so the worker pipeline picks it up. The naive approach — INSERT submissions then `kafkaTemplate.send(...)` — has a window where the row commits but the broker publish fails (network blip, broker GC pause, ack timeout). The contestant sees a 200 with a submissionId; nothing actually queues; their submission vanishes. At 14,000 submissions/sec peak this is not a rare event.

The forcing function: api-gateway must guarantee that every accepted submission row has a corresponding Kafka publish, and that the SPA's 202 response is causally before that publish becomes observable downstream.

## Decision

Use the transactional outbox pattern. The submission INSERT and an outbox INSERT happen in one CRDB transaction. CRDB's native changefeed emits one Kafka record per outbox row commit. The application code never directly calls `kafkaTemplate.send` on the submission ingest path.

Schema sketch:

```sql
CREATE TABLE outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type TEXT NOT NULL,   -- 'Submission'
  aggregate_id  TEXT NOT NULL,     -- submission_id
  event_type    TEXT NOT NULL,     -- 'SubmissionCreated'
  payload       BYTES NOT NULL,    -- proto-encoded SubmissionEvent
  created_at    TIMESTAMPTZ DEFAULT now(),
  published_at  TIMESTAMPTZ
);

CREATE CHANGEFEED FOR TABLE outbox
  INTO 'kafka://...'
  WITH key_in_value, format = 'json', resolved = '5s';
```

The relay (CRDB changefeed) updates `published_at` post-hoc; it isn't on the request path.

## Alternatives considered

**Synchronous Kafka publish.** Fast on the happy path, breaks on broker outage. Atomicity of "DB commit and broker publish" can't be guaranteed without a two-phase commit between two distinct systems.

**Two-phase commit (XA) between CRDB and Kafka.** Neither side supports it well, and the operational cost (hanging in-doubt transactions, recovery procedures) outweighs the benefit. Rejected.

**Application-level retry after publish failure.** Rolls back the submission row on Kafka failure. Loses the contestant's submission attempt; bad UX. Doesn't handle the "we wrote to Kafka but our ack-read failed" branch.

**Custom polling outbox relay** (not changefeed). Roll our own poller that reads `outbox WHERE published_at IS NULL`. Works, but reinvents what CRDB gives for free; adds another component to operate.

## Consequences

**Positive:**
- Atomic write of "the submission exists" + "it is queued for processing".
- No data loss on broker outage; the row sits in `outbox` until the changefeed catches up.
- Reconciliation scanner can be much simpler — it only handles the rare case where the changefeed itself is misbehaving.
- Schema migration is testable in isolation; changefeed is configured once at table creation.

**Negative:**
- Latency: the SPA's 202 returns before the publish has happened (changefeed lag is ~100 ms p99 in practice). Acceptable because the SPA's behaviour doesn't depend on the publish.
- We are now structurally dependent on CRDB-specific changefeed behaviour. Migrating away from CRDB would require introducing Debezium or similar.
- The outbox table grows unbounded; needs a periodic GC of rows where `published_at IS NOT NULL AND published_at < now() - 7 days`.

## Implementation pointers

- Outbox table schema: `database/init.sql`.
- Submission INSERT + outbox INSERT in one tx: `api-gateway/.../service/SubmissionService.java`.
- Changefeed config: applied via `database/init.sql` at first boot; rotated via Flyway when topic conventions change.
- Reconciliation safety net: [`flows/reconciliation-scanner.md`](../flows/reconciliation-scanner.md).
- Outbox GC: TODO (see `tech-spec.md#14`).

## Related

- [`tech-spec.md#8-reliability-mechanisms`](../tech-spec.md#8-reliability-mechanisms)
- [`services/api-gateway.md`](../services/api-gateway.md)
- [`flows/submission-roundtrip.md`](../flows/submission-roundtrip.md) §2 step 4
- [`flows/reconciliation-scanner.md`](../flows/reconciliation-scanner.md)
- [Pattern: Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
