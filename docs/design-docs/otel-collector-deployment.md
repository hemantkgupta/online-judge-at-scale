# OTel Collector Deployment

*Design document for roadmap item 2.6.*

## Problem

Every JVM image in the system bakes the OpenTelemetry Java agent. None of them use it. The compose files set `OTEL_JAVAAGENT_ENABLED=false` in four places — `control-plane-compose.yml:111`, `:158` and `compute-compose.yml:67`, `:114` — because the agent crashes during autoconfigure if no collector is reachable at the configured endpoint. Even with `OTEL_METRICS_EXPORTER=none` the agent attempts to resolve the OTLP target during boot and fails the container with a `NoClassDefFoundError` cascade.

Operators are flying blind. There is no trace from a submission accepted at the gateway through to its verdict in `evaluated_results`. There are no metrics — no Kafka consumer lag gauges, no sandbox pool depth, no per-language compile-time histograms. There is no log aggregation; Docker JSON-file logs land on the local disk and rotate at 100 MB, vanishing on every preemption of the SPOT compute VM.

The fix is well-understood: stand up an [[otel-collector]] that receives OTLP, batches it, and exports to the three GCP-native backends (Cloud Logging, Cloud Monitoring, Cloud Trace), then flip every service's agent on. The non-trivial part is that this is a compose-only deployment with no Kubernetes — so the collector is a sibling container, not a DaemonSet, and the IAM model is the control-plane VM's service account, not a per-pod workload identity.

## Design

### Container choice

Use `otel/opentelemetry-collector-contrib:0.99.0` (pinned major.minor). The Contrib image is required: the upstream `otel/opentelemetry-collector` image ships with the AWS-flavoured exporters and lacks the `googlecloud` exporter and the `googlecloudmonitoring` and `googlecloudlogging` ones. Contrib is larger (~250 MB) and ships exporters we will never use, but it is the only sanctioned image with first-class GCP support.

### Topology

The collector runs as a new service in `control-plane-compose.yml`, alongside api-gateway, problem-service, kafka, cockroachdb, and redis. It is *not* a DaemonSet (we are not on Kubernetes), and it is *not* a sidecar per service (compose has no first-class sidecar concept). It is a single shared instance reachable on the compose-internal network at `oj-otel-collector:4317` (gRPC) and `:4318` (HTTP, unused but exposed for ad-hoc curl debugging).

The compute VM's services (`oj-execution-worker`, `oj-sandbox-manager`) point at the control-plane VM's collector across the internal VPC — `OTEL_ENDPOINT=http://oj-control-plane.c.<project>.internal:4317`. This is a single network hop within the same VPC (`asia-south1`) and adds ~1 ms. The collector batches aggressively (see processor config below) so even sustained 500 RPS of spans across both VMs produces only ~5 KB/s of egress, well below any concern.

A single collector is a SPOF for telemetry but not for the data plane. If the collector dies, the agents buffer in-memory for 30 seconds before dropping; telemetry gaps appear but submissions still succeed. Two-collector HA is a post-launch concern; the relevant alert is "collector pod restarted in the last 5 min".

### Pipeline configuration

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 384
    spike_limit_mib: 96
  batch:
    send_batch_size: 1024
    send_batch_max_size: 2048
    timeout: 5s
  resourcedetection:
    detectors: [gcp, env]
    timeout: 2s

exporters:
  googlecloud:
    project: online-judge-hk
    log:
      default_log_name: oj-otel-default
    sending_queue:
      enabled: true
      num_consumers: 4
      queue_size: 200
  googlemanagedprometheus:
    project: online-judge-hk

extensions:
  health_check:
    endpoint: 0.0.0.0:13133

service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, resourcedetection, batch]
      exporters: [googlecloud]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, resourcedetection, batch]
      exporters: [googlemanagedprometheus]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, resourcedetection, batch]
      exporters: [googlecloud]
  telemetry:
    logs:
      level: info
    metrics:
      address: 0.0.0.0:8888
```

Notes on choices:

- **`memory_limiter` first.** If the collector itself runs out of memory it cannot drop gracefully; the limiter is the only thing that prevents a runaway exporter queue from OOM-killing the container.
- **`batch` timeout 5 s.** Cloud Trace ingestion bills per-write-API-call; 5 s batching cuts cost by an order of magnitude versus the default 200 ms. Latency penalty is acceptable for backend traces.
- **`googlemanagedprometheus` over `googlecloud` for metrics.** GMP is the recommended path for OTel metrics into Cloud Monitoring as of 2025-Q4; it produces Prometheus-native time series that Grafana can query directly via the GMP frontend.
- **`resourcedetection: [gcp]`** stamps every signal with the instance ID and zone, which makes the per-VM split obvious in dashboards.

### Compose entry

```yaml
oj-otel-collector:
  image: otel/opentelemetry-collector-contrib:0.99.0
  container_name: oj-otel-collector
  restart: unless-stopped
  command: ["--config=/etc/otel/config.yaml"]
  volumes:
    - ./otel-collector-config.yaml:/etc/otel/config.yaml:ro
    - /var/run/secrets/google:/var/run/secrets/google:ro
  environment:
    GOOGLE_APPLICATION_CREDENTIALS: ""   # leave empty; use ADC via VM SA
  ports:
    - "4317:4317"
    - "8888:8888"   # collector self-metrics (internal scrape)
    - "13133:13133" # health check
  healthcheck:
    test: ["CMD", "wget", "--spider", "-q", "http://localhost:13133/"]
    interval: 30s
    timeout: 5s
    retries: 3
  mem_limit: 512m
  cpus: 0.5
```

Empty `GOOGLE_APPLICATION_CREDENTIALS` is intentional: when ADC is empty, the GCP SDK falls back to the GCE metadata server, picking up `control_plane_sa@…iam.gserviceaccount.com` automatically. No service-account key file on disk — same posture as the existing GCS signer fetch.

### IAM

Today the control-plane VM's SA has:

- `roles/logging.logWriter` (per `infra/gcp/terraform/main.tf`)
- `roles/secretmanager.secretAccessor`
- `roles/storage.objectViewer`

Add via Terraform:

```hcl
resource "google_project_iam_member" "control_plane_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.control_plane.email}"
}

resource "google_project_iam_member" "control_plane_trace_writer" {
  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.control_plane.email}"
}
```

`roles/cloudtrace.agent` is the dedicated role for trace ingestion (`logWriter` does not transitively grant trace writes — that was a 2024 misconception). `roles/monitoring.metricWriter` is needed for the GMP exporter path.

### Service-side flip

Set in both compose files:

```yaml
environment:
  OTEL_JAVAAGENT_ENABLED: "true"
  OTEL_SERVICE_NAME: api-gateway          # vary per service
  OTEL_EXPORTER_OTLP_ENDPOINT: "http://oj-otel-collector:4317"
  OTEL_EXPORTER_OTLP_PROTOCOL: grpc
  OTEL_RESOURCE_ATTRIBUTES: "deployment.environment=prod,service.namespace=online-judge"
  OTEL_METRICS_EXPORTER: otlp
  OTEL_TRACES_EXPORTER: otlp
  OTEL_LOGS_EXPORTER: otlp
  OTEL_TRACES_SAMPLER: parentbased_traceidratio
  OTEL_TRACES_SAMPLER_ARG: "0.1"           # 10% head sampling
```

10% head sampling on traces is the launch-window default. Errors are always sampled regardless via the Java agent's built-in error-biased sampler.

### Dashboards

Three dashboards ship on day one, all defined as JSON checked into `infra/observability/dashboards/`:

**Submission funnel.** Four-panel timing breakdown:

| Panel              | Source                                                                  | Display     |
|--------------------|-------------------------------------------------------------------------|-------------|
| Accept→Outbox p50/p99 | `oj_submission_accept_latency_seconds` (histogram, in api-gateway)   | line, 1h    |
| Outbox→Lease p50/p99  | span `lease.acquire` start − span `submission.accept` start          | line, 1h    |
| Lease→Exec done    | span `exec.run` duration on the worker                                  | line, 1h    |
| Exec→Verdict published | span `verdict.publish` end − span `exec.run` end                    | line, 1h    |

**Sandbox pool depth per language.** Three gauges sourced from `sandbox_pool_warm_count{language="python|cpp|java"}` published every 5 s by `oj-sandbox-manager`. Threshold lines at `target` and `target/2`.

**Kafka consumer lag.** One panel per consumer group (`execution-worker.pretest`, `execution-worker.system`, `leaderboard-service.evaluated`, `scoring-pipeline.evaluated`) using the OTel Java agent's built-in Kafka instrumentation, which already produces `kafka.consumer.lag` gauge per partition.

## Implementation phases

**Phase A (1d) — collector container.** Add `oj-otel-collector` to `control-plane-compose.yml` and the config file. Add the two IAM bindings in Terraform. Boot it and verify `localhost:13133` health.

**Phase B (1d) — flip api-gateway only.** Set `OTEL_JAVAAGENT_ENABLED=true` on api-gateway alone. Verify traces appear in Cloud Trace and metrics in GMP. This is the smoke test before the bulk flip.

**Phase C (1d) — flip remaining services.** problem-service, execution-worker, sandbox-manager. Verify cross-service trace continuity — a submission span should chain from api-gateway → kafka → execution-worker → sandbox-manager.

**Phase D (2d) — dashboards.** Build the three dashboards. Sanity-check each against a synthetic load run (50 concurrent submissions for 10 minutes).

**Phase E (1d) — alerts.** Cloud Monitoring alerts: collector pod restart, p99 accept→verdict latency >30 s, sandbox pool depth at 0 for >60 s, any consumer-group lag >10,000.

## Risks

**Memory pressure on the control-plane e2-medium.** 4 GB system memory hosts JVM-with-384MB-heap × 2, CRDB single-node, Kafka, Redis, ZK, and now a 512 MB-limited collector. We are within 200 MB of the ceiling. If OOM-killer fires, the collector is the right thing to lose — it has no data-plane criticality and a restart is harmless. Verify via `docker stats` after the bulk flip that resident set is under 3.5 GB.

**Cardinality explosion in metrics.** OTel Java agent's HTTP server instrumentation emits one time series per `(method, route, status_code)` tuple. With Spring's path-templating mis-applied, this can degenerate to one series per request URL. See [[cardinality-explosion]]. Mitigation: explicit `OTEL_INSTRUMENTATION_HTTP_SERVER_ROUTE_FROM_REQUEST` config plus a denylist in the collector's `metricstransform` processor for the worst offenders. Budget: ≤2000 unique label combinations across the system.

**Sampling miss on a real incident.** 10% head sampling means the trace for the one slow submission that mattered may not survive. Mitigation post-launch: switch to a tail sampler ([[tail-vs-head-sampling]]) deployed as a second collector tier. Not on the day-zero list.

**Egress to managed Prometheus during a contest.** GMP ingestion is best-effort metered. A sustained 50K samples/min during a contest pulls ~1 GB/month into the GMP free-tier limit. Cost is rounding-error but worth a budget alert.

## Acceptance criteria

1. `docker compose ps oj-otel-collector` reports `healthy`.
2. A submission round-trip produces a trace in Cloud Trace with at least four spans: `submission.accept`, `outbox.publish`, `lease.acquire`, `exec.run`.
3. The "submission funnel" dashboard renders p50 and p99 over the last hour with non-zero data after a 10-minute synthetic-load test.
4. The "sandbox pool depth" dashboard shows the configured targets (`python:2 / cpp:1 / java:1`) under steady-state.
5. Kafka consumer lag for `execution-worker.pretest` is visible and tracks `kafka-consumer-groups --describe` output within 5 s.
6. Logs from every JVM service appear in Cloud Logging under `logName="projects/online-judge-hk/logs/oj-otel-default"` within 10 s of emission.
7. Killing the collector (`docker compose kill oj-otel-collector`) does not affect submission throughput; the worker continues consuming and publishing verdicts.

## Related

- [[otel-collector]] — the collector itself
- [[distributed-tracing]] — concept this enables
- [[observability]] — broader context
- [[otel-semantic-conventions]] — span/attribute naming
- [[tail-vs-head-sampling]] — future improvement
- [[log-trace-correlation]] — what we get free from `traceId` injection
- [[exemplars-in-histograms]] — connecting metric anomalies back to traces
