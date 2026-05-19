# =============================================================================
# Online Judge — multi-region GCP deployment substrate.
#
# Topology: two VMs in two regions, each running the FULL application stack
# (control plane + compute on every node). Provisions, in stopped state:
#   * A custom VPC with two regional subnets:
#       - oj-subnet-primary    (var.primary_region, 10.10.0.0/24)
#       - oj-subnet-secondary  (var.secondary_region, 10.20.0.0/24)
#   * Firewall rules: IAP SSH + cross-region intra-stack traffic
#   * Artifact Registry repository for the service Docker images (single,
#     hosted in the primary region)
#   * One shared region-VM service account (oj-region) — both VMs run the
#     same software so split SAs would only duplicate IAM bindings
#   * Two VMs:
#       - oj-region-a: n2-standard-2 SPOT, nested virt enabled, runs the full
#         stack (Kafka, CRDB, Redis, all JVM services + sandbox-manager +
#         execution-worker with Firecracker / Docker / gVisor backends)
#       - oj-region-b: identical shape, identical software, different region
#   * Scheduler SA + Cloud Scheduler jobs that stop both VMs at 23:00 IST
#
# CRDB runs as a 2-node multi-region cluster: each node joins the peer over
# the private internal IPs and advertises locality=region=<region>,zone=local
# so range placement honours geography.
# =============================================================================

# ---------- VPC + regional subnets ------------------------------------------

resource "google_compute_network" "oj_vpc" {
  name                    = "oj-vpc"
  auto_create_subnetworks = false
  # GLOBAL routing so cross-region private-IP traffic doesn't take a hairpin
  # over the public internet — the two regional subnets are stitched into one
  # routing fabric and CRDB Raft / Kafka can talk over RFC1918 addresses.
  routing_mode = "GLOBAL"
}

locals {
  regions = {
    a = {
      region      = var.primary_region
      zone        = var.primary_zone
      subnet_name = "oj-subnet-primary"
      subnet_cidr = "10.10.0.0/24"
      instance    = "oj-region-a"
    }
    b = {
      region      = var.secondary_region
      zone        = var.secondary_zone
      subnet_name = "oj-subnet-secondary"
      subnet_cidr = "10.20.0.0/24"
      instance    = "oj-region-b"
    }
  }

  # Full AR repo URL — passed into the startup script so docker-compose can
  # pull `${AR_URL}/api-gateway:latest` without hard-coding the project ID.
  # The AR repository itself lives in the primary region; cross-region pulls
  # work fine, GCP just charges a small egress + a few ms of latency on
  # cold pulls. Acceptable for a dev-grade two-region setup.
  ar_url = "${var.primary_region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_name}"

  ssh_key_entry = "${var.ssh_user}:${file(pathexpand(var.ssh_public_key_path))}"
}

resource "google_compute_subnetwork" "oj_subnets" {
  for_each      = local.regions
  name          = each.value.subnet_name
  ip_cidr_range = each.value.subnet_cidr
  region        = each.value.region
  network       = google_compute_network.oj_vpc.id

  # Lets Identity-Aware Proxy reach the VMs without public IPs. Logging stays
  # off (free) since this is a dev-grade setup.
  private_ip_google_access = true
}

# ---------- Static internal IPs --------------------------------------------
# Each VM's startup-script template carries the OTHER VM's IP (for CRDB
# --join, Kafka advertised-listener cross-region reach, leaderboard-service
# peer fan-out, api-gateway region-mismatch 307 redirect). Letting Compute
# Engine assign ephemeral IPs creates a terraform cycle: oj_region_a's
# metadata references oj_region_b.network_interface[0].network_ip and vice
# versa, so neither resource can be created first. We reserve the IPs up
# front via google_compute_address — addresses are known at plan time and
# both VMs reference the address resources, not each other.
resource "google_compute_address" "region_internal" {
  for_each     = local.regions
  name         = "${each.value.instance}-internal"
  region       = each.value.region
  subnetwork   = google_compute_subnetwork.oj_subnets[each.key].id
  address_type = "INTERNAL"
}

# ---------- Firewall rules --------------------------------------------------

# Allow Identity-Aware Proxy to SSH into both VMs. Source range is the fixed
# block GCP publishes for IAP TCP forwarding.
resource "google_compute_firewall" "allow_iap_ssh" {
  name      = "oj-allow-iap-ssh"
  network   = google_compute_network.oj_vpc.id
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["oj-vm"]
}

# Cross-region intra-stack ingress. Source is restricted to the two regional
# subnet CIDRs (10.10.0.0/24 + 10.20.0.0/24) and target is the `oj-vm` tag, so
# nothing public can hit these ports. The earlier rev opened `0-65535/tcp+udp`
# between the subnets which was easy to reason about but flagged as L2 debt in
# tech-spec §14 — replaced here with an explicit allow-list of the four TCP
# ports that legitimately cross subnets, plus ICMP for diagnostics.
#
# Per-port matrix (see tech-spec §10.1.1 for prose):
#   tcp/26257 — CRDB SQL + Raft (--join peer, cross-region range placement)
#   tcp/9092  — Kafka PLAINTEXT_HOST listener (advertised on the internal IP
#               for cross-region MirrorMaker 2 / parity; in-region clients go
#               through the docker network on :29092, which never traverses
#               this firewall)
#   tcp/8088  — api-gateway peer redirect (APP_PEER_GATEWAY_URL)
#   tcp/8082  — leaderboard-service peer fan-out (APP_PEER_LEADERBOARD_URL)
#
# Everything else (Redis :6379, OTLP :4317, ClickHouse :8123/:9000, sandbox-
# manager :9100, execution-worker actuator :8081, problem-service :8089,
# contest-service :8084, CRDB admin :8080, OTel self-metrics :8888/:13133)
# is intra-VM only — it lives on the docker bridge network on each host and
# does not cross the subnet boundary, so no firewall rule is needed.
#
# UDP is no longer allowed: the stack has no cross-region UDP traffic. ICMP
# stays open so `ping` from one VM to the other works for triage.
resource "google_compute_firewall" "allow_cross_region" {
  name      = "oj-allow-cross-region"
  network   = google_compute_network.oj_vpc.id
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports = [
      "26257", # CRDB SQL + Raft
      "9092",  # Kafka PLAINTEXT_HOST listener
      "8088",  # api-gateway peer redirect
      "8082",  # leaderboard-service peer fan-out
    ]
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = [for k, v in local.regions : v.subnet_cidr]
  target_tags   = ["oj-vm"]
}

# ---------- Artifact Registry ----------------------------------------------

resource "google_artifact_registry_repository" "oj_images" {
  location      = var.primary_region
  repository_id = var.artifact_repo_name
  format        = "DOCKER"
  description   = "Docker images for online-judge services (api-gateway, execution-worker, etc.)"
}

# ---------- Region VM service account --------------------------------------
# Both VMs run the same software so they share one SA. This collapses what
# used to be control_plane_sa + compute_sa into a single identity and
# eliminates duplicated IAM bindings.

resource "google_service_account" "region_sa" {
  account_id   = "oj-region"
  display_name = "Online Judge — region VM (full stack: control plane + compute)"
}

# AR reader so docker-compose can pull every service image.
resource "google_artifact_registry_repository_iam_member" "region_ar_reader" {
  location   = google_artifact_registry_repository.oj_images.location
  repository = google_artifact_registry_repository.oj_images.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.region_sa.email}"
}

# Union of the old two SAs' project-level roles.
locals {
  region_sa_project_roles = [
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter", # OTel googlemanagedprometheus exporter
    "roles/cloudtrace.agent",        # OTel googlecloud trace exporter
  ]
}

resource "google_project_iam_member" "region_sa_roles" {
  for_each = toset(local.region_sa_project_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.region_sa.email}"
}

# ---------- Execution Agent source tarball ---------------------------------
# Zips the in-repo `infra/firecracker/agent/` directory and injects it as a
# base64 string into the VM's startup-script metadata. Roughly 12 KB
# base64-encoded — well under the 256 KB per-metadata-entry limit.
data "archive_file" "agent_src" {
  type        = "zip"
  output_path = "${path.module}/.terraform-agent-src.zip"
  source_dir  = "${path.module}/../../../infra/firecracker/agent"
}

# ---------- JWT secret for api-gateway --------------------------------------
# Generated once and held in terraform state. `tofu destroy` + `tofu apply`
# mints a fresh one — that's fine for a dev-grade setup.
resource "random_password" "jwt_secret" {
  length      = 48
  special     = false
  min_lower   = 8
  min_upper   = 8
  min_numeric = 8
}

# ---------- Region VMs ------------------------------------------------------
# Both VMs are identical except for region/zone and the peer-IP wiring.
# Declared as two separate resources (not for_each) because each needs to
# reference the OTHER's network_interface[0].network_ip in its startup-script
# metadata — for_each + self-reference is a terraform footgun.

resource "google_compute_instance" "oj_region_a" {
  name         = local.regions.a.instance
  machine_type = var.region_machine_type
  zone         = local.regions.a.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.oj_subnets["a"].id
    # Static internal IP allocated up-front via google_compute_address to
    # break the metadata cycle (see oj_subnets comment block above).
    network_ip = google_compute_address.region_internal["a"].address
    # Ephemeral external IP for OUTBOUND only — Docker install, AR auth,
    # Firecracker download, OTel collector pull all hit the public internet.
    # INBOUND stays IAP-only via oj-allow-iap-ssh.
    access_config {}
  }

  service_account {
    email  = google_service_account.region_sa.email
    scopes = ["cloud-platform"]
  }

  # Both VMs now run Firecracker, so BOTH need nested KVM.
  advanced_machine_features {
    enable_nested_virtualization = true
  }

  scheduling {
    provisioning_model          = var.region_use_spot ? "SPOT" : "STANDARD"
    preemptible                 = var.region_use_spot
    automatic_restart           = var.region_use_spot ? false : true
    instance_termination_action = var.region_use_spot ? "STOP" : null
  }

  metadata = {
    ssh-keys               = local.ssh_key_entry
    enable-oslogin         = "FALSE"
    block-project-ssh-keys = "TRUE"

    startup-script = templatefile("${path.module}/../startup/region.sh.tpl", {
      ar_url                  = local.ar_url
      region                  = local.regions.a.region
      zone                    = local.regions.a.zone
      peer_region             = local.regions.b.region
      peer_internal_ip        = google_compute_address.region_internal["b"].address
      jwt_secret              = random_password.jwt_secret.result
      compose_yaml_b64        = base64encode(file("${path.module}/../compose/region.yml"))
      gcs_bucket_name         = google_storage_bucket.oj_test_cases.name
      gcs_signer_secret       = google_secret_manager_secret.problem_service_signer_key.secret_id
      otel_config_yaml_b64    = base64encode(file("${path.module}/../compose/otel-collector-config.yaml"))
      seccomp_profile_b64     = base64encode(file("${path.module}/../../../infra/seccomp/sandbox-seccomp.json"))
      rootfs_init_b64         = base64encode(file("${path.module}/../../../infra/firecracker/rootfs/init.sh"))
      rootfs_builder_b64      = base64encode(file("${path.module}/../../../infra/firecracker/rootfs/build-rootfs.sh"))
      agent_src_zip_b64       = filebase64(data.archive_file.agent_src.output_path)
      sandbox_backend         = var.sandbox_backend
      sandbox_docker_runtime  = var.sandbox_docker_runtime
      linux_hardening_enabled = var.linux_hardening_enabled
      project_id              = var.project_id
    })
  }

  tags = ["oj-vm", "oj-region-a"]

  allow_stopping_for_update = true
}

resource "google_compute_instance" "oj_region_b" {
  name         = local.regions.b.instance
  machine_type = var.region_machine_type
  zone         = local.regions.b.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.oj_subnets["b"].id
    network_ip = google_compute_address.region_internal["b"].address
    access_config {}
  }

  service_account {
    email  = google_service_account.region_sa.email
    scopes = ["cloud-platform"]
  }

  advanced_machine_features {
    enable_nested_virtualization = true
  }

  scheduling {
    provisioning_model          = var.region_use_spot ? "SPOT" : "STANDARD"
    preemptible                 = var.region_use_spot
    automatic_restart           = var.region_use_spot ? false : true
    instance_termination_action = var.region_use_spot ? "STOP" : null
  }

  metadata = {
    ssh-keys               = local.ssh_key_entry
    enable-oslogin         = "FALSE"
    block-project-ssh-keys = "TRUE"

    # NOTE: region-b's startup metadata references region-a's IP. Terraform
    # builds a graph where region-a is created first (region-b depends on it
    # via this reference), and region-a's metadata references region-b's IP
    # — which forces a metadata-update on region-a once region-b's IP is
    # known. In practice this means `tofu apply` runs in two passes:
    #   pass 1: create both VMs, region-a gets a placeholder peer_internal_ip
    #           that resolves to the unknown value of region_b. This becomes
    #           a known-after-apply for region_a's startup-script metadata.
    #   The DAG ordering: region-b is created BEFORE region-a (region-a's
    #   metadata references region-b's IP), so the second metadata-update on
    #   region-a is unnecessary. region-b is created with an unknown
    #   peer-IP for region-a, which terraform also resolves before this
    #   resource is provisioned. The net effect is correct.
    # If startup-script ordering ever becomes a problem (a VM boots before
    # its peer has an IP), the fix is metadata-only update via
    # google_compute_instance_metadata + a cyclic dependency break — out of
    # scope for now.
    startup-script = templatefile("${path.module}/../startup/region.sh.tpl", {
      ar_url                  = local.ar_url
      region                  = local.regions.b.region
      zone                    = local.regions.b.zone
      peer_region             = local.regions.a.region
      peer_internal_ip        = google_compute_address.region_internal["a"].address
      jwt_secret              = random_password.jwt_secret.result
      compose_yaml_b64        = base64encode(file("${path.module}/../compose/region.yml"))
      gcs_bucket_name         = google_storage_bucket.oj_test_cases.name
      gcs_signer_secret       = google_secret_manager_secret.problem_service_signer_key.secret_id
      otel_config_yaml_b64    = base64encode(file("${path.module}/../compose/otel-collector-config.yaml"))
      seccomp_profile_b64     = base64encode(file("${path.module}/../../../infra/seccomp/sandbox-seccomp.json"))
      rootfs_init_b64         = base64encode(file("${path.module}/../../../infra/firecracker/rootfs/init.sh"))
      rootfs_builder_b64      = base64encode(file("${path.module}/../../../infra/firecracker/rootfs/build-rootfs.sh"))
      agent_src_zip_b64       = filebase64(data.archive_file.agent_src.output_path)
      sandbox_backend         = var.sandbox_backend
      sandbox_docker_runtime  = var.sandbox_docker_runtime
      linux_hardening_enabled = var.linux_hardening_enabled
      project_id              = var.project_id
    })
  }

  tags = ["oj-vm", "oj-region-b"]

  allow_stopping_for_update = true
}

# ---------- problem-service: GCS bucket + signer SA + Secret Manager --------
#
# Project-global resources (unchanged from the single-region setup). The
# bucket location stays in the primary region; cross-region reads work fine
# for the secondary-region problem-service. Signer key flows through Secret
# Manager (auto-replication) so both VMs fetch it locally on each boot.

resource "google_storage_bucket" "oj_test_cases" {
  name                        = "oj-test-cases-${var.project_id}"
  location                    = upper(var.primary_region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

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

resource "google_storage_bucket_iam_member" "signer_object_viewer" {
  bucket = google_storage_bucket.oj_test_cases.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.problem_service_signer.email}"
}

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

# Region SA can pull the signer JSON on every boot from either VM.
resource "google_secret_manager_secret_iam_member" "region_signer_accessor" {
  secret_id = google_secret_manager_secret.problem_service_signer_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.region_sa.email}"
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

# One auto-shutdown job per region VM. Each scheduler job lives in the same
# region as the VM it stops — Cloud Scheduler is regional and the job's
# region is independent of the target's region in the URI.
resource "google_cloud_scheduler_job" "auto_shutdown_region_a" {
  name        = "oj-auto-shutdown-region-a"
  description = "Nightly stop of oj-region-a (safety net)"
  schedule    = var.auto_shutdown_cron
  time_zone   = var.auto_shutdown_timezone
  region      = local.regions.a.region

  http_target {
    http_method = "POST"
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${local.regions.a.zone}/instances/${google_compute_instance.oj_region_a.name}/stop"

    oauth_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

resource "google_cloud_scheduler_job" "auto_shutdown_region_b" {
  name        = "oj-auto-shutdown-region-b"
  description = "Nightly stop of oj-region-b (safety net)"
  schedule    = var.auto_shutdown_cron
  time_zone   = var.auto_shutdown_timezone
  region      = local.regions.b.region

  http_target {
    http_method = "POST"
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${local.regions.b.zone}/instances/${google_compute_instance.oj_region_b.name}/stop"

    oauth_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}
