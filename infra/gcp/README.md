# Online Judge on GCP — Operator Walkthrough

Deployment substrate for testing **Firecracker** and **gVisor** sandbox
backends on real cloud x86_64 hardware. Built for blog-grade benchmarking,
not production — designed to cost ~$8/month at ~10 hrs/week of testing
during the GCP $300 free trial and after.

> **First-time provisioning, or rebuilding from scratch?** Walk
> [`setup.md`](./setup.md) top-to-bottom. It covers the GCP project,
> billing, API enablement, Terraform SA + key, and the recreation
> playbook for every "I tore this down to save money" / "I lost my key"
> / "the project got auto-deleted" scenario. This README assumes that
> work is done.

## Topology

```
   asia-south1-a (Mumbai)
   ─────────────────────

   ┌────────────────────── oj-vpc / oj-subnet (10.0.0.0/24) ────────────────────┐
   │                                                                            │
   │   oj-control-plane (e2-medium, 4 GB)            oj-compute (n2-standard-2, │
   │   ┌──────────────────────────┐                  8 GB, SPOT, nested-virt)   │
   │   │ docker-compose:          │                  ┌────────────────────────┐ │
   │   │  • kafka :9092           │   intra-VPC      │ execution-worker       │ │
   │   │  • cockroachdb :26257    │ ◀──────────────▶ │  ├─ Docker backend     │ │
   │   │  • redis :6379           │                  │  ├─ Firecracker backend│ │
   │   │  • api-gateway :8088     │                  │  └─ gVisor (runsc)     │ │
   │   └──────────────────────────┘                  │ /dev/kvm + microVMs    │ │
   │                                                  └────────────────────────┘ │
   └────────────────────────────────────────────────────────────────────────────┘

   ▲ IAP SSH from your Mac (no public IPs on the VMs)
```

## Stages

| Stage | What it adds | Status |
|---|---|---|
| 1 | GCP project + APIs + Terraform SA (pre-reqs done in your shell) | external |
| **2** | **VPC + 2 VMs (stopped) + Artifact Registry + auto-shutdown scheduler** | **this directory** |
| **3** | **Service Dockerfiles + image push pipeline → Artifact Registry** | **this directory** |
| 4 | VM startup scripts (compose-up + Firecracker / gVisor install) | pending |
| 5 | `app.sandbox.docker.runtime` Java config → pass `--runtime=runsc` to Docker | pending |
| 6 | `up.sh` / `down.sh` / `teardown.sh` wrappers | pending |
| 7 | First deploy: terraform apply → push images → start → smoke test | pending |

## Stage 2 — Run the Terraform

Pre-reqs you should already have from the earlier setup conversation:

- GCP project `online-judge-hk` (or whatever ID you set in `terraform.tfvars`)
- Billing linked and active (`billingEnabled: true`)
- APIs enabled: `compute`, `artifactregistry`, `secretmanager`, `cloudscheduler`, `iam`, `iamcredentials`
- Service account `terraform@…iam.gserviceaccount.com` with project-level admin roles
- Key file at `~/.gcp/terraform-key.json`
- `GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/terraform-key.json` exported in your shell
- `tofu` 1.6+ installed (`tofu version`)

### 1. Customise (optional)

```sh
cd infra/gcp/terraform
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars      # change project_id, region, or cost knobs if needed
```

If you're happy with the defaults (Mumbai, e2-medium + n2-standard-2 spot,
25 GB disks, SSH key at `~/.ssh/id_rsa.pub`, login user `hemant`), you can
skip this — Terraform reads the defaults from `variables.tf`.

### 2. Initialise

```sh
tofu init
```

Downloads the Google provider plugin. ~30 seconds. You should see
`OpenTofu has been successfully initialized!`.

### 3. Preview the plan

```sh
tofu plan
```

Should report **~17 resources to create** and **0 to change / destroy**:
- VPC + subnet
- 2 firewall rules
- 1 Artifact Registry repo
- 3 service accounts (2 VMs + 1 scheduler)
- 4 IAM bindings (AR reader x2, log writer x2, scheduler instance admin)
- 2 VMs
- 2 scheduler jobs (auto-shutdown)

Skim the output. Anything that looks wrong → adjust `terraform.tfvars` and
re-plan.

### 4. Apply

```sh
tofu apply
```

Type `yes` when prompted. Takes ~2–3 minutes total: the VMs themselves
provision and stop within ~90 seconds; the App Engine app is the slowest
piece.

When it finishes, `tofu output` prints the useful follow-ups:

```
artifact_registry_url     = "asia-south1-docker.pkg.dev/online-judge-hk/oj-images"
compute_internal_ip       = "10.0.0.3"
control_plane_internal_ip = "10.0.0.2"
ssh_control_plane         = "gcloud compute ssh hemant@oj-control-plane --zone=asia-south1-a --tunnel-through-iap"
ssh_compute               = "gcloud compute ssh hemant@oj-compute --zone=asia-south1-a --tunnel-through-iap"
start_control_plane       = "gcloud compute instances start oj-control-plane --zone=asia-south1-a"
start_compute             = "gcloud compute instances start oj-compute --zone=asia-south1-a"
stop_all                  = "gcloud compute instances stop oj-control-plane oj-compute --zone=asia-south1-a"
```

### 5. Sanity check

```sh
# Both VMs should exist in TERMINATED (stopped) state
gcloud compute instances list

# Artifact Registry repo should be there
gcloud artifacts repositories list

# Auto-shutdown scheduler jobs
gcloud scheduler jobs list --location=asia-south1
```

At this point: infra is up but everything is stopped. No charges accruing
beyond persistent disk (~$5/mo). Stages 4–6 wire in the actual services.

### 6. Tear-down (optional, when done)

```sh
tofu destroy
```

Removes everything. Useful at the end of the project, or before pushing
a major terraform change. Takes ~2 minutes.

After teardown, **everything except the persistent disks is gone**.
Bringing it back up later is just `tofu apply` again — see
[`setup.md` §A](./setup.md#a-after-tofu-destroy) for the playbook.

---

## Stage 3 — Build the service images and push to Artifact Registry

Goal: get the api-gateway + execution-worker images into the AR repo Stage
2 created. The VMs will pull from there at boot in Stage 4.

### What's where

| File | Purpose |
|---|---|
| [`api-gateway/Dockerfile`](../../api-gateway/Dockerfile) | Multi-stage build: Gradle → fat jar → JRE runtime. |
| [`execution-worker/Dockerfile`](../../execution-worker/Dockerfile) | Same shape + the `docker` CLI + `curl` (worker needs to spawn sibling sandboxes via the host Docker socket and drive the Firecracker REST socket). |
| [`infra/gcp/images/push-images.sh`](images/push-images.sh) | Discovers AR URL from `tofu output`, authenticates docker → AR, runs `docker buildx build --platform=linux/amd64 --push` for each service. |
| [`infra/gcp/compose/control-plane-compose.yml`](compose/control-plane-compose.yml) | Stack the control-plane VM runs (zookeeper, kafka, cockroachdb, redis, api-gateway). |
| [`infra/gcp/compose/compute-compose.yml`](compose/compute-compose.yml) | Stack the compute VM runs (execution-worker only — privileged, with /dev/kvm + firecracker mounts). |
| [`.dockerignore`](../../.dockerignore) (repo root) | Keeps build/, .gradle/, IDE config, etc. out of the build context so `docker buildx` doesn't ship 500 MB up to the daemon. |

### Run the build + push

You're on Apple Silicon (ARM64), targeting `linux/amd64` on GCP. The script
uses `docker buildx` which transparently runs the cross-platform build
under QEMU. **First build takes 5–8 minutes per image; subsequent builds
hit Gradle's layered cache and finish in ~90 seconds.**

```sh
# From the repo root. Docker Desktop (or OrbStack) must be running.
./infra/gcp/images/push-images.sh
```

What you should see:

```
[push-images] discovered AR URL: asia-south1-docker.pkg.dev/online-judge-hk/oj-images
[push-images] configuring docker auth for asia-south1-docker.pkg.dev
[push-images] =====================================================
[push-images] building api-gateway → …/api-gateway:latest
[push-images] =====================================================
… buildx output …
[push-images] ✓ api-gateway pushed as …/api-gateway:latest
[push-images]   also tagged …/api-gateway:<git-sha>
[push-images] =====================================================
[push-images] building execution-worker → …/execution-worker:latest
[push-images] =====================================================
…
[push-images] ✓ execution-worker pushed as …
```

### Verify

```sh
AR_URL=$(cd infra/gcp/terraform && tofu output -raw artifact_registry_url)
gcloud artifacts docker images list "${AR_URL}"
```

You should see two images, each with two tags (`latest` + the short git
SHA at push time).

### Test the images locally (optional but recommended)

Before deploying to GCP, sanity-check that the images can boot at all:

```sh
AR_URL=$(cd infra/gcp/terraform && tofu output -raw artifact_registry_url)
# api-gateway needs Kafka + CRDB + Redis from somewhere; easiest is the existing
# Mac docker-compose.yml. Spin it up first:
docker compose up -d zookeeper kafka cockroachdb redis

# Then try to boot the AR image (it'll pull from cloud):
docker run --rm --network=host \
    -e SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    -e SPRING_DATASOURCE_URL='jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable' \
    -e SPRING_DATA_REDIS_HOST=localhost \
    -e APP_JWT_SECRET=dev-only-secret-please-rotate-in-prod-32+chars-needed-for-HS256 \
    "${AR_URL}/api-gateway:latest"
```

Should reach `Started ApiGatewayApplication in X.X seconds`. Ctrl+C to
stop. If this works locally, the same image will work on the control-plane
VM with the same env vars pointed at the in-VPC service names.

### What this stage does NOT do

- Doesn't deploy anything to the VMs. That's Stage 4 (startup scripts that
  install Docker, pull from AR, and run docker-compose).
- Doesn't install Firecracker or gVisor binaries. Also Stage 4 — those
  live on the compute VM's host filesystem, mounted into the worker
  container.

---

## Cost notes

While VMs are **stopped**: ~$5/mo total (just the disks + AR storage).
While VMs are **running**: ~$0.13/hour combined (control plane $0.034/hr
on-demand + compute $0.026/hr spot, plus the always-on disks).

If you're not actively testing, just `gcloud compute instances stop …` (or
wait for the 23:00 IST auto-shutdown). The next `start` resumes with full
disk state intact.

## Security posture (dev-grade)

- **No public IPs** on either VM. SSH only through IAP tunneling.
- **No project-wide SSH keys** — only the key you injected via `ssh-keys`
  metadata gets accepted.
- **Service accounts scoped to least-privilege** — AR reader + log writer,
  nothing else.
- **Auto-shutdown scheduler** has `compute.instanceAdmin.v1` (needed to call
  `instances.stop`) but no other roles.

What this setup does **not** cover: vulnerability scanning, organisation
policies, VPC Service Controls, customer-managed encryption keys, audit-
log retention. All appropriate for a personal blog-grade project; would
need work for anything multi-tenant.
