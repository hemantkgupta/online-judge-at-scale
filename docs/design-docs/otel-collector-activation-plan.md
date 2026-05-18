# OTel Collector activation — implementation plan

> **Status (2026-05-18).** Follow-up to [`otel-collector-deployment.md`](./otel-collector-deployment.md). The collector container, config file, and IAM are already in the repo (`infra/gcp/compose/region.yml::oj-otel-collector`, `infra/gcp/compose/otel-collector-config.yaml`, `infra/gcp/terraform/main.tf::region_sa_project_roles`). [`tech-spec.md`](../tech-spec.md) §9 marks the work as "Implemented, awaiting operator activation". This document is the **final activation slice** that lets the operator flip a single env var and have everything light up.

---

## 1. What's left

| Design-doc item | State today | This change |
|---|---|---|
| Collector container + config + IAM | Done | — |
| Per-service `OTEL_*` env block (sampler, exporters, protocol) | **Only 4 vars wired** — `OTEL_JAVAAGENT_ENABLED`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`. The remaining 6 (`OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_METRICS_EXPORTER`, `OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER`, `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`) are not set, so the agent will default to OTLP-gRPC anyway, but head sampling and the explicit exporter selection from the design doc are *unconfigured*. | Add the missing six vars to every JVM service block in `region.yml`. Same vars on every service — extract into a YAML anchor (`x-otel-defaults`) so adding a future service doesn't drift. |
| Three dashboards (`infra/observability/dashboards/`) | **Directory does not exist.** | Ship the three Cloud Monitoring dashboard JSON files: submission funnel, sandbox pool depth, kafka consumer lag. |
| Cloud Monitoring alerts (Phase E) | **Not started.** | Ship five alert policies under `infra/observability/alerts/`: collector pod restart, p99 accept→verdict > 30 s, sandbox pool depth = 0 for 60 s, consumer-group lag > 10 000, collector OOM-kill. |
| Operator activation flip | `/opt/oj/.env` is materialised from the Terraform-rendered `region.sh.tpl` with `OTEL_JAVAAGENT_ENABLED=false`. | Keep the default at `false` — flipping to `true` after the collector is verified healthy is the deliberate two-step rollout from the design doc. Add an `infra/observability/README.md` operator-flip runbook so the activation is one Terraform var change + a `compose up -d`. |

Out of scope (tracked elsewhere):

- Application-specific metric instrumentation (the names in tech-spec.md §9.3 that `WorkerMetrics` etc. don't yet register). The agent's free JVM + HTTP + Kafka metrics are what the dashboards need; bespoke metrics are a follow-up.
- Tail sampling — design doc §"Risks" defers to post-launch.
- Two-collector HA — design doc §"Topology" defers explicitly.

---

## 2. Concrete deliverables

```
infra/gcp/compose/region.yml                              # add x-otel-defaults YAML anchor; reference it on each JVM service
infra/observability/README.md                             # operator runbook for the .env flip
infra/observability/dashboards/submission-funnel.json
infra/observability/dashboards/sandbox-pool-depth.json
infra/observability/dashboards/kafka-consumer-lag.json
infra/observability/alerts/collector-pod-restart.json
infra/observability/alerts/accept-to-verdict-p99.json
infra/observability/alerts/sandbox-pool-empty.json
infra/observability/alerts/consumer-group-lag.json
infra/observability/alerts/collector-oom-kill.json
infra/observability/scripts/validate.sh                   # JSON sanity, otelcol --dry-run for the YAML
infra/observability/scripts/apply-dashboards.sh           # `gcloud monitoring dashboards create --config-from-file=…`
infra/observability/scripts/apply-alerts.sh               # `gcloud alpha monitoring policies create --policy-from-file=…`
docs/services/observability.md                            # new owner page (collector, agent flip, dashboards, alerts, runbooks)
docs/tech-spec.md §9                                      # flip "TODO: dashboards" / "Implemented, awaiting operator activation" to "Shipped — flip OTEL_JAVAAGENT_ENABLED=true to activate"
docs/design-docs/otel-collector-activation-plan.md        # this file
```

## 3. Implementation order

1. **Plan doc** — this file. (≤200 lines, kept terse.)
2. **`x-otel-defaults` anchor in `region.yml`** — single-source the per-service env block. Verify with `python3 -c "import yaml; yaml.safe_load(open('region.yml'))"` and `docker compose -f region.yml config` if the daemon is reachable.
3. **Dashboards JSON** — author each, validate with `python3 -c "import json; json.load(open(…))"`. Schema-check against the public Cloud Monitoring `mql_dashboard_layout` shape (best-effort, no remote API call from CI).
4. **Alerts JSON** — same pattern as dashboards.
5. **`validate.sh`** — wraps the JSON parse + a `yamllint`-style check. If `otelcol-contrib` binary is reachable (it is from the GitHub release tarball; verified in the sandbox), `otelcol --config=… --dry-run` validates the pipeline graph.
6. **`apply-dashboards.sh` + `apply-alerts.sh`** — shell wrappers around the `gcloud monitoring …` commands so an operator (or future Terraform) can re-apply idempotently.
7. **Observability owner page** + tech-spec.md §9 / §11.4 reconciliation.

## 4. Test plan

| # | Test | Pass criterion |
|---|---|---|
| V1 | `python3 -c "import yaml; yaml.safe_load(open('infra/gcp/compose/region.yml'))"` parses | Exit 0 |
| V2 | `docker compose -f region.yml config -q` (if daemon reachable) emits no errors | Exit 0 — proves the YAML anchor resolves on every service |
| V3 | `python3 -c "import json; json.load(open(…))"` on every dashboard/alert | Exit 0 — well-formed JSON |
| V4 | `otelcol-contrib --config=otel-collector-config.yaml --dry-run` (against the existing config, run by `validate.sh`) | Exit 0 — pipeline graph valid |
| V5 | Each dashboard JSON contains the panel set from the design doc (e.g. submission-funnel ⊃ 4 panels with the named widgets) | Smoke grep |
| V6 | Each alert JSON declares the right `conditions.conditionThreshold.filter` and a `notificationChannels` placeholder | Smoke grep |
| V7 | `validate.sh` is idempotent — run twice, same output | Exit 0 both times |

V1–V7 are local-CI gates; the integration test (collector receives a real span, GCP shows it) is the operator flip itself and runs against a live GCP project, not in this PR.

## 5. Risks

- **Cloud Monitoring dashboard schema drift.** The JSON I ship targets the v1 `cloud-monitoring-dashboards` shape current in May 2026. If `gcloud monitoring dashboards create` evolves before activation, the apply may fail with a clean error — the JSON itself is forward-compatible for the simple chart layouts we use (line + gauge).
- **Per-service `OTEL_RESOURCE_ATTRIBUTES` collisions with the anchor.** The YAML anchor pattern means every service inherits the *same* `OTEL_RESOURCE_ATTRIBUTES`. Each block then *adds* its own `OTEL_SERVICE_NAME` after the anchor — verified with `docker compose config` that the per-service value wins.
- **Operator forgets to apply the IAM step** before flipping the env. Already in Terraform (`region_sa_project_roles`), so a `terraform apply` is the precondition; the README calls this out as step 0.

## 6. Acceptance criteria

The five from the design doc carry over. Additional CI-level criteria:

- (a) Every JVM service in `region.yml` resolves to the same 10-var `OTEL_*` env block when `docker compose config` is run, plus its own `OTEL_SERVICE_NAME`.
- (b) Three dashboard JSON files exist and pass `validate.sh`.
- (c) Five alert JSON files exist and pass `validate.sh`.
- (d) `validate.sh` exits 0 against the repo's pinned `otelcol-contrib` version.
