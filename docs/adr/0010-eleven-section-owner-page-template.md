# ADR-0010: 11-Section Service Owner-Page Template

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

When the project grew from 3 services to 8, the per-service documentation drifted in shape: api-gateway had a deep auth section but no metrics list; execution-worker had a runbook but no failure-modes table; sandbox-manager had a 1500-word section on Firecracker boot but no schema. A new engineer joining the team couldn't predict where to look for a particular kind of information on each page.

The forcing function: docs are read most often by people who have to find one specific thing fast (the metric name, the config property, the runbook for the symptom they see). A predictable structure is more valuable than per-page expressiveness.

## Decision

Adopt a strict 11-section template for every per-service owner page. Each page is the authoritative spec for one service. Cross-cutting contracts live in [`tech-spec.md`](../tech-spec.md); the owner page is for implementation details.

The 11 sections, in this exact order:

1. **Purpose** — what this service owns; what it doesn't.
2. **External interfaces** — REST endpoints, Kafka topics consumed/produced, outbound HTTP, listening surface.
3. **Internal design** — key mechanisms specific to this service.
4. **Data ownership** — which tables/topics/keys this service writes vs reads.
5. **Failure modes** — concrete failure-to-detection-to-behaviour table.
6. **Configuration reference** — every property + default + purpose.
7. **Metrics emitted** — name + type + labels + meaning.
8. **Runbook** — common incidents with diagnose + fix.
9. **Tests & verification** — unit + integration + manual smoke.
10. **Relevant design docs** — pointers to `design-docs/` for forward-looking context.
11. **Code map** — concern → file mapping.

When adding a new service, copy this structure. When updating an existing page, keep the structure stable.

## Alternatives considered

**Free-form per-service.** Faster to write. Lost the ability to skim across services for a comparable section. New engineers reported needing 3-5x longer to find a metric name or a failure mode on an unfamiliar service.

**Strict template but shorter (5-6 sections).** Collapsing Runbook into Failure Modes lost the operational specificity. Collapsing Configuration into Internal Design lost the per-property table that operators reference. Each section earned its keep.

**Spec-as-code (auto-generated from annotations).** Higher fidelity to current code; lower writing cost. Operationally heavy: requires a new build step, schema annotations on every method, and the spec becomes only as good as the annotations. The Runbook section especially resists automated generation because it captures path-dependent operational learning.

**Different templates per service type** (transactional vs streaming vs gateway). Sounds good on paper. In practice, every service had enough commonality that one template worked. The cost of three templates would have been: which one applies to a new service? Skipped.

## Consequences

**Positive:**
- A reader who knows the template knows where to look on any page. The metric name is in §7. The runbook is in §8. The file paths are in §11.
- Authoring is faster — the structure tells you what to write next.
- Cross-page consistency is auditable: "every owner page has a Failure Modes table" is a lint check.
- Drift between services and tech-spec surfaces during page-authoring (e.g. tech-spec §5.4 had a Redis key name that no service page mentioned — the discrepancy was caught the first time the owner page was written).

**Negative:**
- Some sections feel forced for the thinnest services (analytics-pipeline has minimal data ownership; the section is one bullet). Acceptable.
- The template is more verbose than a minimal README, so the four heavy services are 3-4K words each. Reading time is real.
- When a section's content genuinely should be reorganised (e.g. the Internal design section growing past 2K words and wanting subsections), the structure forces nested headings rather than restructuring.

## Implementation pointers

- The template description lives at [`services/README.md`](../services/README.md) under "Page structure".
- The eight current owner pages: `services/api-gateway.md`, `services/execution-worker.md`, `services/sandbox-manager.md`, `services/problem-service.md`, `services/contest-service.md`, `services/leaderboard-service.md`, `services/scoring-pipeline.md`, `services/analytics-pipeline.md`.

## Related

- [`services/README.md`](../services/README.md)
- The disagreement-resolution rule between tech-spec and an owner page is also documented at the end of `services/README.md`: "the owner page is authoritative for implementation details; the tech-spec is authoritative for cross-cutting contracts."
