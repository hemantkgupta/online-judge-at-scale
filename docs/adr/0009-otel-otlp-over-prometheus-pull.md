# ADR-0009: OpenTelemetry (OTLP) over Prometheus Pull

**Status**: Accepted
**Date**: 2026-05-19
**Deciders**: Engineering team

## Context

The system needs metrics, distributed traces, and structured logs in production. There are three pillars to wire and a constellation of choices: SDKs per language, agent vs sidecar vs library, push (OTLP, statsd) vs pull (Prometheus), per-pillar backend (Cloud Monitoring + Cloud Trace + Cloud Logging, or Prometheus + Tempo + Loki, or Datadog).

Two additional constraints:
- Multi-region: the JVM services and the Go agent in microVMs need to emit telemetry from any region. Cross-region scraping (Prometheus pull from a central server) is operationally awkward: firewall holes, regional outage handling, scrape interval drift.
- The Firecracker microVM is short-lived (~seconds). Pull-based scraping cannot catch a VM that boots, runs, and dies between scrape intervals.

## Decision

Use OpenTelemetry SDKs in every service (Java agent + manual instrumentation; Go SDK in the in-guest agent and `oj-vsock-client`). All telemetry is exported via OTLP (gRPC) to an OTel Collector running per-region. The Collector batches and routes:
- Metrics → GCP Cloud Monitoring (via the GCP exporter).
- Traces → GCP Cloud Trace.
- Logs → GCP Cloud Logging.

Per-host telemetry (sandbox-manager, compute-VM metrics) also goes through the Collector. The full design lives in [`design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md).

## Alternatives considered

**Prometheus + remote_write.** Each JVM service exposes `/actuator/prometheus`; a per-region Prometheus scrapes and ships to a central long-term store via remote_write. Workable but adds Prometheus as a separate operated component, requires every service to maintain a scrape endpoint and the labels to be stable, and doesn't naturally cover short-lived microVM emitters.

**Datadog Agent.** All-in-one. Vendor lock-in. Cost is significant at the OJ's expected metric/trace volume.

**Direct GCP Cloud Monitoring SDK from Java.** The Cloud Monitoring Java SDK is heavyweight, designed primarily for batch monitoring; instrumenting traces/logs separately would duplicate effort. Doesn't compose well with the Go agent.

**StatsD or DogStatsD.** Push-based, supports short-lived emitters. Metrics only — no traces, no logs. Would force a second mechanism for the other two pillars.

## Consequences

**Positive:**
- One SDK family across Java + Go. Common semantic conventions for resource attributes (region, service.name, etc.).
- The Collector decouples emission from backend. Switching backends (GCP → on-prem Prometheus, or Datadog) is a Collector config change, not a code change.
- Push model handles short-lived microVMs naturally.
- Per-region Collector means a single regional outage isolates its own telemetry.

**Negative:**
- The OTel Java agent has its own startup cost (~1-2 s on cold boot) and overhead (~5% CPU at high throughput). Quantified in the launch dashboard.
- The Collector is yet another component to operate (compose service on the control-plane VM; production: per-region Deployment).
- OTel SDKs have changed wire format historically (OTLP/HTTP vs OTLP/gRPC); version pinning matters.

## Implementation pointers

- OTel Collector compose config: `infra/otel-collector/otel-collector-config.yaml`.
- JVM instrumentation: each service's `application.yml` has `management.opentelemetry.*` and `otel.exporter.otlp.endpoint`.
- Naming convention: `oj.<subsystem>.<metric>.<unit>{labels}` for metrics; trace span names follow OTel semantic conventions.
- Submission-funnel dashboard: GCP Cloud Monitoring dashboard JSON in `infra/dashboards/`.
- The metrics catalog for execution-worker: [`services/execution-worker.md#7`](../services/execution-worker.md).

## Related

- [`design-docs/otel-collector-deployment.md`](../design-docs/otel-collector-deployment.md)
- [`tech-spec.md#9-observability`](../tech-spec.md#9-observability)
- All [`services/*`](../services/) — each owner page §7 documents the metrics that service emits.
