# OTel Collector activation runbook

The collector container, config file, and Terraform IAM are already in the repo (`infra/gcp/compose/region.yml::oj-otel-collector`, `infra/gcp/compose/otel-collector-config.yaml`, `infra/gcp/terraform/main.tf::region_sa_project_roles`). This directory holds the **dashboards + alerts + apply scripts** that finish the deployment, plus the operator runbook below. The broader observability story — Java-agent baked into every JVM image, custom metrics registered by `WorkerMetrics`/`SandboxMetrics`, the smoke-test entry point — lives in [`README.md`](./README.md).

Background: [`../../docs/design-docs/otel-collector-deployment.md`](../../docs/design-docs/otel-collector-deployment.md) and [`../../docs/design-docs/otel-collector-activation-plan.md`](../../docs/design-docs/otel-collector-activation-plan.md).

```
infra/observability/
├── README.md                                 ← broader observability (custom metrics catalogue, agent config)
├── activation-runbook.md                     ← this file
├── dashboards/
│   ├── submission-funnel.json                ← accept → outbox → lease → verdict, p50/p99
│   ├── sandbox-pool-depth.json               ← per-language warm pool gauges
│   └── kafka-consumer-lag.json               ← per-group lag with the alert source
├── alerts/
│   ├── collector-pod-restart.json
│   ├── collector-oom-kill.json
│   ├── accept-to-verdict-p99.json            ← > 30 s p99 latency for 5 min
│   ├── sandbox-pool-empty.json               ← any language at 0 for 60 s
│   └── consumer-group-lag.json               ← any group > 10 000 for 5 min
└── scripts/
    ├── validate.sh                           ← offline sanity gate (CI-friendly)
    ├── apply-dashboards.sh                   ← gcloud monitoring dashboards create/update
    └── apply-alerts.sh                       ← gcloud alpha monitoring policies create/update
```

## Operator runbook — first activation

Two-step rollout is deliberate. Flipping the agent on against an unreachable collector crashes the JVM at autoconfigure (known footgun — see the design doc §Problem).

### Step 0 — IAM is in Terraform; apply if you haven't

```bash
cd infra/gcp/terraform && terraform apply
```

Confirms `roles/{logging.logWriter, monitoring.metricWriter, cloudtrace.agent}` on `region_sa`.

### Step 1 — collector container

The collector is part of the `oj-region` systemd unit. On a fresh VM it comes up with `OTEL_JAVAAGENT_ENABLED=false`, so nothing is yet sending it traffic.

```bash
ssh oj-region-a
sudo systemctl status oj-region
docker compose -f /opt/oj/region.yml ps oj-otel-collector
# wait until healthcheck reports healthy
curl -sf http://localhost:13133/ && echo " collector healthy"
```

### Step 2 — flip the JVM agents on

```bash
ssh oj-region-a
sudo sed -i 's/^OTEL_JAVAAGENT_ENABLED=false/OTEL_JAVAAGENT_ENABLED=true/' /opt/oj/.env
sudo systemctl restart oj-region    # bounces every service with the new env
```

Repeat on `oj-region-b`. Within ~30 s, spans should be visible in Cloud Trace and metrics in GMP.

### Step 3 — dashboards + alerts

From your laptop (or any host with `gcloud` auth):

```bash
./scripts/validate.sh
PROJECT_ID=online-judge-hk ./scripts/apply-dashboards.sh
# Provision notification channels first if you want non-empty wiring:
NOTIFY_CHANNEL_IDS="projects/online-judge-hk/notificationChannels/123,projects/online-judge-hk/notificationChannels/456" \
PROJECT_ID=online-judge-hk \
    ./scripts/apply-alerts.sh
```

Both apply scripts are idempotent — they match on `displayName` and update in place.

### Step 4 — verification

Walks the seven acceptance criteria from `docs/design-docs/otel-collector-deployment.md`:

1. `docker compose ps oj-otel-collector` → `healthy`.
2. Submit a problem via the API; the trace in Cloud Trace shows at least `submission.accept`, `outbox.publish`, `lease.acquire`, `exec.run`.
3. Submission funnel dashboard renders p50/p99 over the last hour after a 10-min synthetic-load test.
4. Sandbox pool depth dashboard shows the configured per-language targets under steady state.
5. Kafka consumer lag dashboard tracks `kafka-consumer-groups.sh --describe` within 5 s.
6. Cloud Logging shows entries under `logName="projects/online-judge-hk/logs/oj-otel-default"`.
7. `docker compose kill oj-otel-collector` → submissions still succeed (the data plane is independent of the collector).

## Rolling back

The agent is fail-shut by design. To disable:

```bash
sudo sed -i 's/^OTEL_JAVAAGENT_ENABLED=true/OTEL_JAVAAGENT_ENABLED=false/' /opt/oj/.env
sudo systemctl restart oj-region
```

Dashboards and alert policies can be removed via `gcloud monitoring dashboards delete <NAME>` and `gcloud alpha monitoring policies delete <NAME>` respectively; the JSON in this directory is the canonical source if you want to re-apply later.

## CI / pre-merge gate

`scripts/validate.sh` is the cheap offline gate — runs in under a second, no GCP credentials needed. It checks:

- `region.yml` parses and every JVM service inherits the full 10-var OTel env block (the `x-otel-defaults` anchor) plus its own `OTEL_SERVICE_NAME`.
- `otel-collector-config.yaml` parses (and if `otelcol-contrib` is on `$PATH`, that the pipeline graph builds).
- Every dashboard/alert JSON parses and has the required top-level fields.
