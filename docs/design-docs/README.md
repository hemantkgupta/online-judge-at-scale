# Online Judge — Production-Readiness Design Docs

Standalone technical specifications for the heavyweight items on the [Online Judge production-readiness roadmap](../oj-prod-readiness-roadmap.md). Each doc is concrete enough that an engineer can implement directly without further research. Single-day tractable items from the roadmap are covered separately by per-PR design notes and are not duplicated here.

## Contents

| Doc | Roadmap section | Summary |
|-----|-----------------|---------|
| [auth-end-to-end.md](./auth-end-to-end.md) | 2.1 | Real signup/login with Argon2id, refresh-token rotation with family revocation, JWT key versioning via Secret Manager, audit table, per-`/auth/*` rate limits isolated from submission limits. |
| [otel-collector-deployment.md](./otel-collector-deployment.md) | 2.6 | Deploy `otel/opentelemetry-collector-contrib` as a compose sibling on the control-plane VM, route traces/metrics/logs to Cloud Trace/Monitoring/Logging via the VM SA, flip every JVM agent on, ship three launch dashboards. |
| [kafka-cluster-and-crdb-cluster.md](./kafka-cluster-and-crdb-cluster.md) | 2.7 | Move both stateful systems off SPOF: 3-broker KRaft Kafka (drop Zookeeper) and 3-node CRDB with mutual TLS, co-located on three new e2-standard-2 data VMs across three zones, with a stop-the-world migration window. |
| [microvm-egress-lockdown.md](./microvm-egress-lockdown.md) | 3.1 | Per-microVM Linux network namespace with no interfaces except loopback, host iptables UID-DROP rules as defense-in-depth, vsock as the only path that survives, nightly validation harness per language. |
| [ci-cd-github-actions.md](./ci-cd-github-actions.md) | 3.11 | Three GitHub Actions workflows (PR, build-and-push, deploy) authenticated via Workload Identity Federation, SHA-tagged images in Artifact Registry, two-phase deploy via gcloud-over-IAP, branch protection on `main`. |
| [react-spa-and-websockets.md](./react-spa-and-websockets.md) | 4.20 | Vite + React + TypeScript SPA with monaco-editor, hosted on Cloud Storage + Cloud CDN, integrated with api-gateway via REST and STOMP-over-SockJS for live verdict push, with autosave and reconnect-with-poll-fallback. |
| [contest-close-system-tests-replay.md](./contest-close-system-tests-replay.md) | 4.23 | Contest-close lifecycle event triggers a batched replay of every ACCEPTED-pretest submission onto `submissions.system`, with a `SYSTEM_TESTED` transition gating the scoring pipeline's final-leaderboard recompute. |
| [multi-region-rollout.md](./multi-region-rollout.md) | section 5 | Three regions (asia-south1, us-east1, europe-west1), CRDB multi-region with RBR submissions and GLOBAL problems, per-region Kafka with no cross-region mirror, Cloud DNS geo-routing with health-check fallback, ~3.5× cost. |
| [key-rotation.md](./key-rotation.md) | 3.3, 3.4 | Versioned-kid JWT keys + GCS V4 signer SA key rotation. Monthly Cloud Scheduler → Cloud Function → Secret Manager → on-disk sidecar with in-process hot-reload. 7-day overlap window; ≤ 2 min end-to-end propagation. Closes tech-spec §14 M2. |

## How to use

Each doc is self-contained. Read the relevant doc fully before starting implementation. Risks and acceptance criteria sit at the bottom; treat the acceptance criteria as the launch-readiness checklist for that workstream.

When implementing, cross-reference the wiki pages linked in each doc's "Related" section. The [Online Judge blog post](../online-judge-at-scale.md) and the [execution-service deep dive](../execution-service-deep-dive.md) are the architectural narrative this set of docs operationalises.

## Items deliberately not given a design doc

The roadmap has ~40 items. Many are single-day code edits that don't need a separate spec:

- **3.6** Submission source-code size cap — one validator annotation on `SubmissionRequest`.
- **2.4** VerdictEvent per-ordinal breakdown — proto field add + producer/consumer wiring, well-described inline in the roadmap.
- **2.3** Per-problem time/memory limits plumbing — straightforward field-pass-through across three services.
- **2.5** Idempotency reclaim count + poison transition — adds one column and one threshold check; the design is in the roadmap text.
- **3.2** Rate-limit tuning; **3.5** firewall narrowing — each is a config or terraform change. (JWT + signer-key rotation, formerly here as 3.3 / 3.4, has graduated to its own design doc above.)
- **3.7-3.10, 3.13-3.17** Autoscaling, SPOT drain, reconciliation scanner, log driver, runbooks, image caching, language pre-touch, KVM contention — each is a discrete operational task with the approach already named.

Those items can be picked up directly from the roadmap and worked as PRs. The eight docs in this directory are the ones whose surface area or cross-cutting nature warrants standalone specs.
