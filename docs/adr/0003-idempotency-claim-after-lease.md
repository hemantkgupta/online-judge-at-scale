# ADR-0003: Claim Idempotency AFTER Lease, Not Before

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team
**Supersedes**: implicit pre-rollout design (claim-then-lease)

## Context

The execution-worker drives a multi-step pipeline per submission: parse → fetch test cases → lease sandbox → dispatch to agent → publish verdict. Kafka delivery is at-least-once; redelivery is normal under any of: worker crash, consumer-group rebalance, intentional nack, broker hiccup. The system therefore needs an idempotency mechanism so each `(submission_id, phase)` produces exactly one verdict on `evaluated_results`.

The choice is *when* the worker writes to `idempotency_keys` relative to leasing a sandbox.

The original design (now superseded) was **claim-then-lease**: insert the idempotency row first as the very first step of `processSubmission`, then ask sandbox-manager for a VM. The reasoning was "we want to detect duplicates BEFORE we burn capacity on a sandbox".

That design failed in production. The failure mode: a burst saturated the sandbox pool. `sandbox-manager` returned `503 pool_exhausted` with `retry_after_ms`. The worker had already claimed the idempotency row (status='processing', attempts=1). It nacked, the row's `created_at` was now in the past, the next redelivery saw an existing row past its lease window, treated it as stale, reclaimed under attempts++. After 5 such cycles (default `app.idempotency.max-attempts=5`), the row went POISON and was dead-lettered. The contestant did nothing wrong; the server just ran out of capacity briefly.

## Decision

Move the idempotency claim to **after** a successful `/lease`. Specifically: lease the sandbox first, then call `IdempotencyService.claimSubmission`. If the lease fails with `PoolExhaustedException`, nack with the broker-supplied `retry_after_ms` and never touch the idempotency table.

```java
// 1. Lease (may throw PoolExhaustedException → caught, nack, no idempotency state)
ExecutionResult result = executionService.execute(...);

// 2. Claim — at this point we KNOW we got a sandbox
IdempotencyService.ClaimDecision claim = idempotencyService.claimSubmission(submissionId, phase);
if (claim.status() == COMPLETED) { ack.acknowledge(); return; }
if (claim.status() == IN_PROGRESS) { ack.nack(Duration.ofSeconds(5)); return; }
if (claim.status() == POISON) { /* DLQ + ack */ return; }
// CLAIMED: proceed
```

## Alternatives considered

**Claim-then-lease (the original).** Failed in production as described above. The attempts counter became coupled to capacity events.

**Claim-only-on-completion.** Skip the in-progress claim entirely; only write a row after the verdict is published. Loses duplicate detection during the worker's execution window: two workers can simultaneously lease + execute + publish two verdicts for the same submission.

**Distributed lock on submissionId.** A Redis SET NX or a CRDB advisory lock. Adds a second consistency mechanism whose failure mode (lock leak on worker crash) is exactly what the idempotency table already protects against. Rejected as redundant.

## Consequences

**Positive:**
- Capacity events (pool_exhausted) are invisible to the attempts counter. A contestant's submission is never DLQ'd because the cluster was busy.
- The attempts counter genuinely counts "real" attempts — failures that happened while the worker held a sandbox.
- Duplicate detection within the lease window is preserved (the post-lease claim catches `IN_PROGRESS`).

**Negative:**
- Race window: between `/lease` and the claim, a second worker can also lease for the same submissionId. Both acquire sandboxes; one wins the claim, the other sees IN_PROGRESS, releases. Wasted lease — small. We accept it.
- Slightly more complex to reason about because the "first thing in the pipeline" is no longer the idempotency check.

## Implementation pointers

- The catch block ordering: `execution-worker/.../consumer/SubmissionConsumer.java#processSubmission`.
- `PoolExhaustedException` flow: `SandboxManagerClient.lease` + `processSubmission` catch.
- Claim state machine: `IdempotencyService.claimSubmission`.
- The bug history is documented inline at [`services/execution-worker.md`](../services/execution-worker.md) §3.3.

## Related

- [`flows/pool-exhausted-backpressure.md`](../flows/pool-exhausted-backpressure.md)
- [`flows/kafka-redelivery-and-idempotency.md`](../flows/kafka-redelivery-and-idempotency.md)
- [`services/execution-worker.md`](../services/execution-worker.md) §3.3
