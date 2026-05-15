# GCP Setup — First-Time Provision & Recreation Playbook

Everything you need to recreate the deployment substrate from scratch, in
order. Designed so that after `tofu destroy` (or even after losing your Mac
or deleting the GCP project) you can rebuild end-to-end by walking this
file top-to-bottom.

> Companion docs:
> - [`README.md`](./README.md) — day-to-day operator commands once the
>   infrastructure exists.
> - [`terraform/`](./terraform/) — the actual OpenTofu source of truth.

---

## Cost rationale (why the design looks the way it does)

| Item | Choice | Why |
|---|---|---|
| Region | `asia-south1` (Mumbai) | Lowest latency from the operator (~30 ms). Slightly higher unit cost than `us-central1` but the difference is &lt;$0.50/mo at this scale. |
| Control-plane VM | `e2-medium` (2 vCPU / 4 GB), **on-demand**, pd-balanced 25 GB | Hosts Kafka + CRDB + Redis + api-gateway as stateful services. Preemption would lose Kafka offsets, so no spot. |
| Compute VM | `n2-standard-2` (2 vCPU / 8 GB), **SPOT**, **nested-virt**, pd-balanced 25 GB | Hosts the execution-worker only. Microvms are ephemeral by design, so preemption is harmless. Nested virt is the whole reason we're here (Firecracker needs `/dev/kvm`). |
| Disks | 25 GB pd-balanced | Holds OS + Docker images + Firecracker artifacts. 50 GB SSD would be $17/mo just for storage; pd-balanced is fast enough. |
| Auto-shutdown | 23:00 IST daily (Cloud Scheduler) | Safety net against "I forgot to stop the VMs." Free. |
| SSH | IAP tunneling only, no public IPs | Smaller attack surface. `gcloud compute ssh --tunnel-through-iap` instead of plain ssh. |
| Image storage | Artifact Registry in `asia-south1` | Pull traffic stays in-region (free). ~$0.20/mo for ~2 GB of images. |
| Secret storage | None yet (Stage 5 if needed) | $10/mo target. Spring Boot reads JWT secret from VM metadata; Secret Manager comes later only if we need it. |

**Estimated monthly cost at ~10 hrs/week of testing: ~$8.** ([README cost
notes](./README.md#cost-notes) has the per-line breakdown.)

---

## Stage 1 — Pre-requisites (run on your Mac)

These set up everything OpenTofu needs to authenticate and provision. Most
of them are one-time per machine; a few are per-project. The
[`Recreation`](#recreation-playbook) section below shows which to skip when
rebuilding.

### 1.1 Install local tools

```sh
brew install --cask google-cloud-sdk    # gcloud CLI
brew install opentofu                   # tofu binary (Apache 2.0 fork of Terraform)

gcloud --version
tofu version
```

### 1.2 Log into GCP

```sh
gcloud auth login                       # opens browser; choose your Google account
```

### 1.3 Create the GCP project + link billing

```sh
PROJECT_ID="online-judge-hk"              # change if you want a different ID

gcloud projects create $PROJECT_ID --name="Online Judge at Scale"
gcloud config set project $PROJECT_ID
```

Billing must be active or the API enablement in 1.4 will fail. From the
console at https://console.cloud.google.com/billing :

1. Make sure at least one billing account is **OPEN** (`gcloud billing
   accounts list` should show `OPEN: True` for at least one row).
2. Link the project to it:

```sh
BILLING_ID="<paste from above>"
gcloud billing projects link $PROJECT_ID --billing-account=$BILLING_ID
gcloud billing projects describe $PROJECT_ID   # confirm billingEnabled: true
```

### 1.4 Enable the GCP APIs

Seven services. Some auto-enable on first use in some accounts and not in
others — listing them all explicitly is the safe path:

```sh
gcloud services enable \
    compute.googleapis.com \
    artifactregistry.googleapis.com \
    secretmanager.googleapis.com \
    cloudscheduler.googleapis.com \
    iam.googleapis.com \
    iamcredentials.googleapis.com \
    cloudresourcemanager.googleapis.com
```

Why each one:

| Service | Used by |
|---|---|
| `compute.googleapis.com` | VPC, subnet, firewall, the two VMs |
| `artifactregistry.googleapis.com` | The Docker image repo |
| `secretmanager.googleapis.com` | Future-Stage secret storage (JWT secret, DB password) |
| `cloudscheduler.googleapis.com` | The 23:00 IST auto-shutdown jobs |
| `iam.googleapis.com` | Creating the per-VM and per-scheduler service accounts |
| `iamcredentials.googleapis.com` | OAuth token generation for the scheduler's HTTP target |
| `cloudresourcemanager.googleapis.com` | `google_project_iam_member` bindings (log writer, instance admin) |

Verify (should print seven lines):

```sh
gcloud services list --enabled --format="value(config.name)" \
  | grep -E 'compute|artifactregistry|secretmanager|cloudscheduler|^iam\.|iamcredentials|cloudresourcemanager'
```

If the `tofu apply` later errors with "Cloud Resource Manager API has not
been used", you missed it — enable it, wait 30 seconds for propagation,
and re-run `tofu apply` (it picks up from the partial state).

> **Why not App Engine API?** Earlier docs (and earlier versions of this
> file) included `appengine.googleapis.com` because Cloud Scheduler used
> to require an App Engine application in the project. That requirement
> was retired for HTTP-target jobs in 2022, and we only create HTTP-target
> jobs here. The `terraform/main.tf` reflects that.

### 1.5 Create the Terraform service account + grant roles

```sh
gcloud iam service-accounts create terraform \
    --display-name="Terraform Service Account"

SA="terraform@${PROJECT_ID}.iam.gserviceaccount.com"

for role in roles/compute.admin \
            roles/iam.serviceAccountAdmin \
            roles/iam.serviceAccountUser \
            roles/artifactregistry.admin \
            roles/secretmanager.admin \
            roles/cloudscheduler.admin \
            roles/resourcemanager.projectIamAdmin; do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SA" \
        --role="$role" \
        --condition=None \
        --no-user-output-enabled
done
```

### 1.6 Generate the key file

```sh
mkdir -p ~/.gcp
gcloud iam service-accounts keys create ~/.gcp/terraform-key.json \
    --iam-account=$SA

# Tell Tofu where to find it. Add to your shell profile so it persists.
echo 'export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/terraform-key.json' >> ~/.zshrc
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/terraform-key.json
```

**The file at `~/.gcp/terraform-key.json` is a credential.** Treat it like
a password. It lives outside any repo on purpose; it is also recoverable —
if you lose it, see [Recreation Playbook §C](#c-after-losing-the-local-key-file).

### 1.7 Verify

```sh
gcloud config list
gcloud services list --enabled --format="value(config.name)" \
  | grep -E 'compute|artifactregistry|secretmanager|cloudscheduler|^iam\.|iamcredentials|cloudresourcemanager'
gcloud iam service-accounts list --filter="email:terraform@*"
ls -la ~/.gcp/terraform-key.json
tofu version
```

All five should print non-empty, non-error output (the services line
should list seven rows). If anything is missing, re-run the relevant
subsection.

---

## Stage 2 — Run Tofu

Day-to-day operator steps live in [`README.md`](./README.md#stage-2--run-the-terraform).
Short version:

```sh
cd infra/gcp/terraform
tofu init
tofu plan
tofu apply           # type "yes"; takes ~2–3 minutes
tofu output          # the IPs, AR URL, and ready-to-paste gcloud commands
```

Once this is done, the [`Recreation Playbook`](#recreation-playbook) below
covers every "I want to bring this back" scenario.

---

## Recreation Playbook

Pick the row that matches what you currently have.

| Scenario | What's gone | What's left | Skip ahead to |
|---|---|---|---|
| **A** — Just ran `tofu destroy` to save money | Cloud resources (VPC, VMs, AR, scheduler) | Local repo + key file + the project itself | [§A](#a-after-tofu-destroy) |
| **B** — Disconnected for weeks; suspect things were deleted | Maybe everything cloud-side | Local repo + key file | [§B](#b-after-an-unknown-amount-of-time) |
| **C** — Lost the key file on a new Mac | Local key file | Repo + project + (maybe) the cloud resources | [§C](#c-after-losing-the-local-key-file) |
| **D** — Project deleted (manually or by GCP after 30 days unused) | Everything | Local repo | [§D](#d-after-the-project-itself-is-gone) |
| **E** — Fresh machine, fresh repo, ground zero | Everything | Nothing | [Stage 1](#stage-1--pre-requisites-run-on-your-mac) |

### §A — After `tofu destroy`

You have the repo, the key file, the project, the billing link, all APIs
enabled. The cloud is empty.

```sh
cd infra/gcp/terraform
tofu apply           # recreates the 18 resources in ~2 min
```

That's it. Same outputs as the first time. Disk state is gone (the disks
were destroyed too), so Docker images need to be re-pushed (Stage 3) and
the VMs reprovisioned (Stage 4) once those land.

### §B — After an unknown amount of time

Same as §A, but verify state first:

```sh
# 1. Are the APIs still enabled?
gcloud services list --enabled --format="value(config.name)" \
  | grep -E 'compute|artifactregistry|secretmanager|cloudscheduler|^iam\.|iamcredentials'

# 2. Is billing still healthy?
gcloud billing projects describe $PROJECT_ID

# 3. Is the terraform SA still there?
gcloud iam service-accounts list --filter="email:terraform@*"

# 4. Does the key file still work?
gcloud auth activate-service-account --key-file=~/.gcp/terraform-key.json
gcloud projects describe online-judge-hk
```

If anything from 1–4 fails, jump to the matching Stage 1 sub-section and
redo it. Then `tofu apply`.

### §C — After losing the local key file

The Terraform SA still exists in GCP, but you can't authenticate as it. You
need to log into GCP with your *user* account and generate a fresh key for
the existing SA.

```sh
# 1. Log in as yourself (browser opens)
gcloud auth login

# 2. Set project
gcloud config set project online-judge-hk

# 3. Generate a new key for the existing terraform SA
mkdir -p ~/.gcp
gcloud iam service-accounts keys create ~/.gcp/terraform-key.json \
    --iam-account=terraform@online-judge-hk.iam.gserviceaccount.com

# 4. Re-export the env var
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/terraform-key.json
echo 'export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/terraform-key.json' >> ~/.zshrc

# 5. Sanity-check then proceed
tofu plan
tofu apply
```

> Old keys for the SA remain valid even after you generate a new one.
> Revoke old ones with `gcloud iam service-accounts keys list --iam-account=... && gcloud iam service-accounts keys delete <OLD_KEY_ID> --iam-account=...`
> if you suspect they leaked.

### §D — After the project itself is gone

Most painful path but still automated. Redo every step of Stage 1 from 1.3
onward (the `gcloud projects create` step), then `tofu apply`. Stage 1
takes ~10 minutes the second time because you know the commands; the
billing link step is the only one that takes the longest.

Special case: GCP auto-deletes projects with no billing/activity for 30+
days. If you didn't intend for this to happen, you can usually restore via
`gcloud projects undelete $PROJECT_ID` within 30 days of deletion. Past
that, you have to recreate the project (which means new `PROJECT_ID` since
the old ID is reserved for 30 more days).

### §E — Fresh machine, fresh everything

[Stage 1 from the top](#stage-1--pre-requisites-run-on-your-mac), then
[Stage 2](#stage-2--run-tofu). Allow ~30 minutes start to finish.

---

## What's in the repo vs. what's outside

This is the inventory the recreation steps assume.

| Item | Lives in | Why |
|---|---|---|
| OpenTofu source | `infra/gcp/terraform/*.tf` (committed) | Source of truth for cloud shape |
| Operator walkthrough | `infra/gcp/README.md` (committed) | Day-to-day commands |
| This setup guide | `infra/gcp/setup.md` (committed) | Stage 1 + recreation playbook |
| Operator's tfvars overrides | `infra/gcp/terraform/terraform.tfvars` (**gitignored**) | Might contain non-default project IDs |
| Tofu state | `infra/gcp/terraform/terraform.tfstate` (**gitignored**) | Tofu's local view of cloud reality |
| GCP service account key | `~/.gcp/terraform-key.json` (**outside repo entirely**) | Credential — never in version control |
| SSH key | `~/.ssh/id_rsa.pub` (**outside repo**) | Operator-specific |
| GCP project (`online-judge-hk`) | GCP itself, not local | Cloud resource |
| GCP billing account | GCP itself, not local | Cloud resource |
| Docker images (Stage 3+) | Artifact Registry, built locally | Cloud resource + build artefact |

The recreation rule of thumb: **anything in the committed repo is enough
to bring back the cloud shape; everything outside it has to be recreated
by walking Stage 1.**

---

## A note on Tofu state

`terraform.tfstate` is **local-only** in this setup. That's fine for a
solo project but means:

- If you ever work on this from a second machine, run `tofu destroy` on
  the first machine first, or move the state.
- Backing up `terraform.tfstate` is a courtesy to your future self. It
  saves a ~30-second `tofu refresh` step on recreation, nothing more.

For a multi-person setup we'd switch to a **GCS remote backend** (also
free at this scale). One file change in `versions.tf` adds it. Not needed
for blog-grade work.

---

## Tear-down

```sh
cd infra/gcp/terraform
tofu destroy             # type "yes"; takes ~2 minutes
```

Removes all 18 resources. After this you pay only for:
- Anything held by your *user* (e.g. a Google Cloud Storage bucket you
  created manually — not applicable to this setup).
- Free-tier reservations like the project itself (which has no cost).

The project, the billing link, the APIs, the Terraform SA, and the local
key file all stay intact. Recreation from here is [§A](#a-after-tofu-destroy).
