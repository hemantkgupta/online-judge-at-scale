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
| **4** | **VM startup scripts (compose-up + Firecracker / gVisor install)** | **this directory** |
| **5** | **`app.sandbox.docker.runtime` Java config → pass `--runtime=runsc` to Docker** | **execution-worker/** |
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

## Stage 4 — VM startup scripts

Goal: every time a VM boots, it installs the host-side tooling it needs
(Docker engine, optionally Firecracker + gVisor on the compute VM),
authenticates to Artifact Registry, drops the compose YAML + `.env`, and
brings up the stack via systemd. Re-running is idempotent.

### What's where

| File | Purpose |
|---|---|
| [`startup/control-plane.sh.tpl`](startup/control-plane.sh.tpl) | Installs Docker + gcloud, drops `control-plane-compose.yml` + `init.sql` + `.env`, enables `oj-control-plane.service`. |
| [`startup/compute.sh.tpl`](startup/compute.sh.tpl) | Installs Docker + Firecracker v1.10.1 + guest kernel + rootfs + gVisor (runsc, registered as a Docker runtime), drops compose + seccomp profile + `.env`, enables `oj-compute.service`. |
| `terraform/main.tf` | Wires both scripts into VM metadata via `templatefile()`. Compose YAML + init.sql + seccomp JSON are base64-encoded into the metadata (no GCS bucket required). |
| `terraform/variables.tf` | New knobs: `sandbox_backend` (docker / firecracker), `sandbox_docker_runtime` (runc / runsc), `linux_hardening_enabled`. Flip these to compare backends without rebuilding images. |
| `random_password.jwt_secret` | 48-char shell-safe secret minted in terraform state. Injected as `APP_JWT_SECRET` into the api-gateway container. |

### How the injection works

Terraform reads the compose YAML / init.sql / seccomp profile off disk at
plan time, base64-encodes them, and stuffs them into the VM's
`startup-script` metadata. On boot, GCE runs the script as root; it
`base64 -d`s the payloads back to `/opt/oj/`. No external object store, no
chicken-and-egg auth bootstrap — the only network call before docker-compose
runs is the `gcloud auth configure-docker` helper, which uses the VM's
attached service account.

### Re-apply

```sh
cd infra/gcp/terraform
tofu plan          # should report: 1 to add (jwt_secret), 2 to change (both VMs)
tofu apply
```

Apply takes ~30 seconds — both VMs are stopped, so the metadata update
applies in-place without a reboot. The scripts will run on the next
`gcloud compute instances start …`.

### First boot

The first boot on each VM downloads packages, so it's slow:

- **control-plane**: ~2–3 min (Docker install + AR auth + image pull + compose-up).
- **compute**: ~5–7 min (Docker + 80 MB Firecracker tarball + 50 MB kernel
  + 250 MB rootfs + gVisor + AR pull). Watch progress in `/var/log/oj-startup.log`.

Subsequent boots are ~30 sec — everything is cached on the persistent disk.

### Verify it worked

After `gcloud compute instances start oj-control-plane`, give it ~3 min then:

```sh
gcloud compute ssh hemant@oj-control-plane --zone=asia-south1-a --tunnel-through-iap

# Inside the VM:
sudo tail -50 /var/log/oj-startup.log     # should end with "control-plane.sh done"
sudo systemctl status oj-control-plane    # should be active (exited)
sudo docker compose -f /opt/oj/control-plane-compose.yml ps   # 5 services Up
curl -s localhost:8088/actuator/health    # api-gateway: {"status":"UP"}
```

Compute VM equivalent:

```sh
gcloud compute instances start oj-compute --zone=asia-south1-a
# wait ~7 min for first-boot install
gcloud compute ssh hemant@oj-compute --zone=asia-south1-a --tunnel-through-iap

sudo tail -50 /var/log/oj-startup.log     # ends with "compute.sh done"
ls /dev/kvm                                # should exist; nested virt is on
/usr/local/bin/firecracker --version       # 1.10.1
runsc --version                            # release-20240826.0
sudo docker info | grep -A2 Runtimes       # runc + runsc both listed
sudo docker logs oj-execution-worker --tail 30
```

### Flipping backends

The whole point of this exercise is comparing Docker / gVisor / Firecracker.
Three knobs in `terraform.tfvars`:

```hcl
sandbox_backend         = "docker"      # or "firecracker"
sandbox_docker_runtime  = "runc"        # or "runsc" (gVisor)
linux_hardening_enabled = false         # or true (seccomp + caps + cgroupns)
```

After editing, `tofu apply` updates the metadata, then on the compute VM:

```sh
sudo systemctl restart oj-compute.service   # picks up new /opt/oj/.env
```

No VM reboot, no rebuild.

### What this stage does NOT do

- Doesn't run a smoke test — that's Stage 7. The services will be up but
  nothing's submitted a problem yet.
- Doesn't include the `up.sh` / `down.sh` wrappers — Stage 6.
- Doesn't add the gVisor runtime *selection* in Java. The compute VM offers
  both `runc` and `runsc` to Docker; Java picks via `app.sandbox.docker.runtime`.
  Stage 5 wires that property through to `DockerExecutionService`.

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
