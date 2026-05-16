# =============================================================================
# Online Judge — GCP deployment substrate.
#
# Provisions, in stopped state:
#   * A custom VPC + subnet (asia-south1 by default)
#   * Firewall rules: IAP SSH (no public IPs) + internal traffic between VMs
#   * Artifact Registry repository for the service Docker images
#   * Two service accounts (one per VM, AR-reader scoped)
#   * Two VMs:
#       - oj-control-plane: e2-medium, runs Kafka/CRDB/Redis/api-gateway in
#         docker-compose
#       - oj-compute:       n2-standard-2 spot, nested virt enabled, runs
#         the execution-worker with Firecracker / Docker / gVisor backends
#   * A scheduler service account
#   * Two Cloud Scheduler jobs that stop the VMs at 23:00 IST daily
#
# All Stage-2 resources are infrastructural only. Docker images and startup
# scripts come in Stages 3 + 4.
# =============================================================================

# ---------- VPC + subnet ----------------------------------------------------

resource "google_compute_network" "oj_vpc" {
  name                    = "oj-vpc"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "oj_subnet" {
  name          = "oj-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.oj_vpc.id

  # Lets Cloud NAT / Identity-Aware Proxy reach the VMs without exposing
  # public IPs. Logging stays off (free) since this is a dev-grade setup.
  private_ip_google_access = true
}

# ---------- Firewall rules --------------------------------------------------

# Allow Identity-Aware Proxy to SSH into both VMs. Source range is the fixed
# block GCP publishes for IAP TCP forwarding; no public IP needed on the VMs.
resource "google_compute_firewall" "allow_iap_ssh" {
  name      = "oj-allow-iap-ssh"
  network   = google_compute_network.oj_vpc.id
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"] # IAP TCP forwarding range
  target_tags   = ["oj-vm"]
}

# Allow all intra-VPC traffic so the compute VM can reach Kafka :9092,
# CockroachDB :26257, Redis :6379 on the control-plane VM.
resource "google_compute_firewall" "allow_internal" {
  name      = "oj-allow-internal"
  network   = google_compute_network.oj_vpc.id
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["oj-vm"]
}

# ---------- Artifact Registry ----------------------------------------------

resource "google_artifact_registry_repository" "oj_images" {
  location      = var.region
  repository_id = var.artifact_repo_name
  format        = "DOCKER"
  description   = "Docker images for online-judge services (api-gateway, execution-worker, etc.)"
}

# ---------- VM service accounts --------------------------------------------

resource "google_service_account" "control_plane_sa" {
  account_id   = "oj-control-plane"
  display_name = "Online Judge — control-plane VM"
}

resource "google_service_account" "compute_sa" {
  account_id   = "oj-compute"
  display_name = "Online Judge — compute VM (Firecracker / gVisor)"
}

# Both VMs need to pull from Artifact Registry.
resource "google_artifact_registry_repository_iam_member" "control_plane_ar_reader" {
  location   = google_artifact_registry_repository.oj_images.location
  repository = google_artifact_registry_repository.oj_images.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.control_plane_sa.email}"
}

resource "google_artifact_registry_repository_iam_member" "compute_ar_reader" {
  location   = google_artifact_registry_repository.oj_images.location
  repository = google_artifact_registry_repository.oj_images.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.compute_sa.email}"
}

# Both VMs write logs to Cloud Logging (free up to 50 GiB/mo).
resource "google_project_iam_member" "control_plane_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.control_plane_sa.email}"
}

resource "google_project_iam_member" "compute_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.compute_sa.email}"
}

# ---------- SSH key injection ----------------------------------------------

locals {
  ssh_key_entry = "${var.ssh_user}:${file(pathexpand(var.ssh_public_key_path))}"

  # Full AR repo URL — passed into startup scripts so docker-compose can pull
  # `${AR_URL}/api-gateway:latest` without hard-coding the project ID.
  ar_url = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_name}"
}

# ---------- Execution Agent source tarball ---------------------------------
# Zips the in-repo `infra/firecracker/agent/` directory and injects it as a
# base64 string into the compute VM's startup-script metadata. The VM
# unzips it under /opt/oj/agent and `build-rootfs.sh` does `go build`
# against it to produce the static linux/amd64 binary that goes into the
# Firecracker harness rootfs. Roughly 12 KB base64-encoded — well under
# the 256 KB per-metadata-entry limit.
data "archive_file" "agent_src" {
  type        = "zip"
  output_path = "${path.module}/.terraform-agent-src.zip"
  source_dir  = "${path.module}/../../../infra/firecracker/agent"
}

# ---------- JWT secret for api-gateway --------------------------------------
# Generated once and held in terraform state. `tofu destroy` + `tofu apply`
# will mint a fresh one — that's fine for a dev-grade setup; production
# would route this through Secret Manager.
resource "random_password" "jwt_secret" {
  length      = 48
  special     = false # keep it shell-safe; the .env file is sourced as-is
  min_lower   = 8
  min_upper   = 8
  min_numeric = 8
}

# ---------- Control-plane VM ------------------------------------------------

resource "google_compute_instance" "control_plane" {
  name         = "oj-control-plane"
  machine_type = var.control_plane_machine_type
  zone         = var.zone
  # NOTE: desired_status intentionally omitted. Power state is driven by the
  # operator (gcloud compute instances start/stop) and the auto-shutdown
  # scheduler — not by terraform. Otherwise `tofu apply` after a startup
  # would silently re-stop the VM you just brought up to test.

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.oj_subnet.id

    # Ephemeral external IP for OUTBOUND only. The compute VM's startup
    # script fetches Docker, apt packages, the OTel agent jar, and the
    # CI vmlinux from the public internet; the control-plane VM fetches
    # Docker + Kafka images at first boot. Without this block the VM has
    # no internet egress (no Cloud NAT, no public IP) and apt + docker
    # GPG curls hang for ~5 min before the startup-script gives up.
    #
    # INBOUND remains IAP-only: oj-allow-iap-ssh firewall restricts
    # source range to 35.235.240.0/20, and no other ingress rule is open.
    # A random internet host pinging the public IP can't reach :22, the
    # actuator ports, or anything else — same security posture as the
    # no-public-IP design. Trade: ~$0.005/hr per running VM ($3.65/mo if
    # 24/7, much less with the 23:00 IST auto-shutdown).
    access_config {}
  }

  service_account {
    email  = google_service_account.control_plane_sa.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    ssh-keys               = local.ssh_key_entry
    enable-oslogin         = "FALSE" # we use injected SSH keys for parity with the Pi setup
    block-project-ssh-keys = "TRUE"  # only the key we put here is accepted

    # Stage 4: bring up Kafka/CRDB/Redis/api-gateway via docker-compose on
    # every boot. Compose YAML + init.sql are base64-injected so we don't
    # need a GCS bucket for them.
    startup-script = templatefile("${path.module}/../startup/control-plane.sh.tpl", {
      ar_url             = local.ar_url
      region             = var.region
      jwt_secret         = random_password.jwt_secret.result
      compose_yaml_b64   = base64encode(file("${path.module}/../compose/control-plane-compose.yml"))
      init_sql_b64       = base64encode(file("${path.module}/../../../database/init.sql"))
      # problem-service V4-signer wiring (see GCS bucket / SA / Secret Manager
      # resources at the top of main.tf). The startup script fetches the JSON
      # key into /opt/oj/gcs-signer.json on every boot.
      gcs_bucket_name    = google_storage_bucket.oj_test_cases.name
      gcs_signer_secret  = google_secret_manager_secret.problem_service_signer_key.secret_id
    })
  }

  tags = ["oj-vm", "oj-control-plane"]

  allow_stopping_for_update = true
}

# ---------- Compute VM (nested-virt + spot) ---------------------------------

resource "google_compute_instance" "compute" {
  name         = "oj-compute"
  machine_type = var.compute_machine_type
  zone         = var.zone
  # See note on the control_plane resource — power state is operator-driven,
  # not terraform-driven.

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.oj_subnet.id

    # Ephemeral external IP for OUTBOUND — same justification as the
    # control-plane VM. Without this the startup script can't fetch
    # Docker, the Firecracker binary, the guest kernel, or the OTel agent
    # jar from the public internet. Inbound stays IAP-only via the
    # oj-allow-iap-ssh firewall rule.
    access_config {}
  }

  service_account {
    email  = google_service_account.compute_sa.email
    scopes = ["cloud-platform"]
  }

  # The thing that makes Firecracker viable on a cloud VM: nested KVM.
  advanced_machine_features {
    enable_nested_virtualization = true
  }

  # Spot pricing for ~70% off. Compute work here is stateless (microVMs are
  # ephemeral, Kafka offsets are on the control plane) so preemption is
  # acceptable.
  scheduling {
    provisioning_model          = var.compute_use_spot ? "SPOT" : "STANDARD"
    preemptible                 = var.compute_use_spot
    automatic_restart           = var.compute_use_spot ? false : true
    instance_termination_action = var.compute_use_spot ? "STOP" : null
  }

  metadata = {
    ssh-keys               = local.ssh_key_entry
    enable-oslogin         = "FALSE"
    block-project-ssh-keys = "TRUE"

    # Stage 4: install Docker + Firecracker + gVisor on first boot, then
    # bring up the execution-worker container. The control-plane VM's
    # internal IP is wired in so the worker knows where to find Kafka/CRDB.
    startup-script = templatefile("${path.module}/../startup/compute.sh.tpl", {
      ar_url                   = local.ar_url
      region                   = var.region
      control_plane_ip         = google_compute_instance.control_plane.network_interface[0].network_ip
      compose_yaml_b64         = base64encode(file("${path.module}/../compose/compute-compose.yml"))
      seccomp_profile_b64      = base64encode(file("${path.module}/../../../infra/seccomp/sandbox-seccomp.json"))
      # The custom Firecracker harness rootfs is built on the compute VM on
      # first boot using these two scripts (init = PID-1 inside the microVM,
      # build-rootfs = host-side debootstrap+pack script).
      rootfs_init_b64          = base64encode(file("${path.module}/../../../infra/firecracker/rootfs/init.sh"))
      rootfs_builder_b64       = base64encode(file("${path.module}/../../../infra/firecracker/rootfs/build-rootfs.sh"))
      # Zipped Execution Agent source — unzipped on the VM and compiled
      # by build-rootfs.sh into a static linux/amd64 binary that goes
      # into the harness rootfs.
      agent_src_zip_b64        = filebase64(data.archive_file.agent_src.output_path)
      sandbox_backend          = var.sandbox_backend
      sandbox_docker_runtime   = var.sandbox_docker_runtime
      linux_hardening_enabled  = var.linux_hardening_enabled
    })
  }

  tags = ["oj-vm", "oj-compute"]

  allow_stopping_for_update = true
}

# ---------- problem-service: GCS bucket + signer SA + Secret Manager --------
#
# The problem-service signs short-lived V4 GCS download URLs and hands them
# to the execution-worker. V4 signing happens client-side in the JVM (no API
# call), which means the JCA needs the SA's private key in PEM/JSON form on
# disk — ADC on a GCE VM CANNOT serve the private key, only access tokens.
#
# Pipeline:
#   1. `google_storage_bucket "oj_test_cases"` holds input/expected-output
#      bytes (one object per ordinal per problem).
#   2. `google_service_account "problem_service_signer"` is the SA whose
#      private key signs the V4 URLs. It needs `roles/storage.objectViewer`
#      on the bucket — that's the scope embedded in the signed URLs.
#   3. `google_service_account_key` mints a JSON key; the raw JSON is
#      base64-decoded into a Secret Manager secret so the VM can fetch it on
#      every boot. (The plaintext key sits in tfstate too — acceptable for
#      this dev-grade setup; production should mint via Workload Identity
#      Federation or use the IAM Signer API.)
#   4. The control-plane VM's existing SA gets
#      `roles/secretmanager.secretAccessor` so its startup script can pull
#      the key into /opt/oj/gcs-signer.json (mode 0400).
#
# The compose file mounts /opt/oj/gcs-signer.json into problem-service at
# /var/secrets/gcs-signer.json (ro), and GCS_SIGNER_KEY_PATH points at it.

resource "google_storage_bucket" "oj_test_cases" {
  # GCS bucket names are GLOBAL. Suffixing with project ID keeps the name
  # collision-free across forks of this terraform.
  name                        = "oj-test-cases-${var.project_id}"
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true # dev-grade: let `tofu destroy` wipe contents

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_service_account" "problem_service_signer" {
  account_id   = "oj-problem-signer"
  display_name = "Online Judge — problem-service V4 URL signer"
  description  = "Whose private key signs short-lived GCS download URLs handed to the execution-worker."
}

# Signer needs READ on the bucket — that's the scope embedded in the signed
# URLs. We do not give it object-create; uploads happen via the operator's
# own credentials (`gcloud storage cp ...`) during problem seeding.
resource "google_storage_bucket_iam_member" "signer_object_viewer" {
  bucket = google_storage_bucket.oj_test_cases.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.problem_service_signer.email}"
}

# JSON key. Stored in tfstate (acceptable for this dev-grade setup; rotate by
# tainting + re-applying). Tofu marks `private_key` as sensitive so it never
# echoes to plan output.
resource "google_service_account_key" "problem_service_signer_key" {
  service_account_id = google_service_account.problem_service_signer.name
  key_algorithm      = "KEY_ALG_RSA_2048"
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

resource "google_secret_manager_secret" "problem_service_signer_key" {
  secret_id = "oj-problem-signer-key"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "problem_service_signer_key_v1" {
  secret      = google_secret_manager_secret.problem_service_signer_key.id
  secret_data = base64decode(google_service_account_key.problem_service_signer_key.private_key)
}

# Control-plane SA can pull the signer JSON on every boot.
resource "google_secret_manager_secret_iam_member" "control_plane_signer_accessor" {
  secret_id = google_secret_manager_secret.problem_service_signer_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.control_plane_sa.email}"
}

# ---------- Auto-shutdown via Cloud Scheduler -------------------------------

resource "google_service_account" "scheduler_sa" {
  account_id   = "oj-scheduler"
  display_name = "Online Judge — auto-shutdown scheduler"
}

resource "google_project_iam_member" "scheduler_instance_admin" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# Cloud Scheduler used to require an App Engine app in the project (the
# notorious "appengine.applications.create" precondition). That requirement
# was retired in 2022 for HTTP-target jobs — which is what we use — so we
# create the scheduler jobs directly. The appengine.googleapis.com API can
# stay disabled.

resource "google_cloud_scheduler_job" "auto_shutdown_control_plane" {
  name        = "oj-auto-shutdown-control-plane"
  description = "Nightly stop of the control-plane VM (safety net)"
  schedule    = var.auto_shutdown_cron
  time_zone   = var.auto_shutdown_timezone
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${var.zone}/instances/${google_compute_instance.control_plane.name}/stop"

    oauth_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

resource "google_cloud_scheduler_job" "auto_shutdown_compute" {
  name        = "oj-auto-shutdown-compute"
  description = "Nightly stop of the compute VM (safety net)"
  schedule    = var.auto_shutdown_cron
  time_zone   = var.auto_shutdown_timezone
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${var.zone}/instances/${google_compute_instance.compute.name}/stop"

    oauth_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}
