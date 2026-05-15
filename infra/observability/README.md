# Observability (Workstream H)

We ship the **OpenTelemetry Java agent** in every Spring Boot image (`api-gateway`,
`execution-worker`, `sandbox-manager`, and — when Workstream A lands —
`problem-service`). The agent auto-instruments Spring MVC, Kafka, JDBC, the
JDK HTTP client, and a long tail of common libraries; you get traces +
metrics for free without touching the code.

## Enabling export

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to a reachable collector address.
**Unset → the agent runs but exports nowhere** (no-op deployment, safe default
when no collector is provisioned).

In `infra/gcp/compose/*-compose.yml` the env var is wired from `OTEL_ENDPOINT`
in `/opt/oj/.env`. Set it to:

| Where | Example |
| --- | --- |
| SigNoz running on the control-plane VM | `http://10.0.0.2:4317` |
| Cloud Trace via the OTel GCP sidecar | `http://otel-collector:4317` |
| A dev Mac running the repo-root compose | `http://host.docker.internal:4317` |

The local-dev SigNoz stack already exists in the repo-root `docker-compose.yml`
(see the `otel-collector` / `signoz` / `clickhouse-signoz` services). It
listens on `:4317` (gRPC) and `:4318` (HTTP).

## Per-service identity

Set per-service in compose (already wired):

```
OTEL_SERVICE_NAME=oj-execution-worker          # or oj-sandbox-manager / oj-api-gateway
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=gcp,region=asia-south1
```

## Custom metrics

| Metric | Fires when | Owning workstream (wire-in) |
| --- | --- | --- |
| `sandbox.lease.latency_ms` (histogram, attr `language`) | Every `LeaseService.lease(...)` invocation, entry → return | H (wired) |
| `sandbox.leases.active` (up-down, attr `language`) | LEASED transition (inc) / TERMINATED transition (dec) | H (wired) |
| `sandbox.watchdog.fires_total` (counter, attr `language`) | Wall-clock-kill SIGKILL fires | **CD** (`WatchdogService.onFire`) |
| `sandbox.pool.ready` (up-down, attr `language`) | Replenisher tick observes READY-queue depth | **CD** (`PoolManager` 500ms loop) |
| `worker.gcs.fetch.latency_ms` (histogram, attr `bucket`) | Around each GCS object GET | **B** (`GcsClient.fetch`) |
| `worker.agent.exec.latency_ms` (histogram, attr `language`) | Around the vsock-client exec | **E** (`AgentClient.exec`) |
| `worker.verdicts.published_total` (counter, attrs `verdict`/`language`/`phase`) | After successful Kafka send to `evaluated_results` | H (wired in `SubmissionConsumer`) |

Class locations:

- `sandbox-manager/src/main/java/com/onlinejudge/sandbox/observability/SandboxMetrics.java`
- `execution-worker/src/main/java/com/onlinejudge/worker/observability/WorkerMetrics.java`

Both classes are Spring `@Component`s — autowire and call the record helpers
from your code. The OTel SDK shipped with the Java agent provides the runtime
`Meter`; tests run against the no-op default (no exporter wired).

## Smoke test

`infra/firecracker/test/end-to-end.sh` walks the whole pipeline: health-check,
POST submission, wait for verdict on `evaluated_results`. Requires `curl`,
`jq`, and either `kcat` or a running `oj-kafka` container. Pass
`--skip-sm-health` when running from outside the compute VM's VPC.
