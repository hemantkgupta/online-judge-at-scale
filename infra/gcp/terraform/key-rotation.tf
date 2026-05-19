# =============================================================================
# Key rotation (tech-spec §14 M2 — "JWT secret + signer SA key both in tfstate,
# no rotation cron").
#
# Two Cloud Scheduler jobs invoke two Cloud Functions on a monthly cadence:
#
#   oj-key-rotation             → oj-rotate-jwt-key      (1st  of month, 02:00 UTC)
#   oj-key-rotation-signer      → oj-rotate-signer-key   (15th of month, 02:00 UTC)
#
# Offsetting by ~14 days means we never stack the JWT and signer-SA rotations
# on the same night — if one rotation breaks, the other path stays healthy and
# the operator has a clean variable to triage.
#
# Both functions write a new version of a Secret Manager secret. On each
# region VM, two systemd timers (`oj-jwt-keys.timer` and `oj-signer-key.timer`)
# mirror `versions/latest` to disk every 60 s. The api-gateway hot-reloads
# the JWT key map via the on-disk file watcher in JwtKeyStore; the
# problem-service is bounced by the timer's post-fetch hook when the file
# bytes change (signer credentials are not hot-reloadable — see the design
# doc for the why).
#
# The 7-day overlap window for JWT keys is a property of the API contract,
# not a terraform knob: the rotation function carries the previous kid into
# the new payload, and the NEXT month's rotation drops it. Since access-token
# TTL is 15 minutes, the worst-case "broken tokens after key change" window
# would be 15 minutes WITHOUT the overlap; WITH it the window is zero.
#
# Initial v1 still comes from `random_password.jwt_secret` in main.tf — the
# Cloud Function bootstraps v2 onto that base on its first invocation. To
# break the tfstate dependency completely (M2's stretch goal) the operator
# can rotate twice, then `tofu state rm random_password.jwt_secret` and zero
# out the now-orphaned env var. Out of scope for this commit.
# =============================================================================

# ---------- Secret: oj-jwt-keys (the JSON payload the gateway reads) -------

resource "google_secret_manager_secret" "jwt_keys" {
  secret_id = "oj-jwt-keys"
  replication {
    auto {}
  }
}

# Region VMs need to read it; rotation function needs to write a new version.
resource "google_secret_manager_secret_iam_member" "region_jwt_keys_accessor" {
  secret_id = google_secret_manager_secret.jwt_keys.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.region_sa.email}"
}

# ---------- Service account: rotation function ------------------------------

resource "google_service_account" "key_rotator_sa" {
  account_id   = "oj-key-rotator"
  display_name = "Online Judge — monthly JWT + signer key rotation"
  description  = "Runs as the oj-rotate-jwt-key + oj-rotate-signer-key Cloud Functions."
}

resource "google_secret_manager_secret_iam_member" "rotator_jwt_writer" {
  secret_id = google_secret_manager_secret.jwt_keys.id
  role      = "roles/secretmanager.secretVersionAdder"
  member    = "serviceAccount:${google_service_account.key_rotator_sa.email}"
}

resource "google_secret_manager_secret_iam_member" "rotator_jwt_reader" {
  # The function reads the LATEST version to discover the current kid.
  secret_id = google_secret_manager_secret.jwt_keys.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.key_rotator_sa.email}"
}

resource "google_secret_manager_secret_iam_member" "rotator_signer_writer" {
  secret_id = google_secret_manager_secret.problem_service_signer_key.id
  role      = "roles/secretmanager.secretVersionAdder"
  member    = "serviceAccount:${google_service_account.key_rotator_sa.email}"
}

# Mint new keys for the signer SA.
resource "google_service_account_iam_member" "rotator_signer_key_admin" {
  service_account_id = google_service_account.problem_service_signer.name
  role               = "roles/iam.serviceAccountKeyAdmin"
  member             = "serviceAccount:${google_service_account.key_rotator_sa.email}"
}

# ---------- Cloud Function source: oj-rotate-jwt-key -----------------------
#
# Packaged from `infra/gcp/key-rotation/rotate_jwt_key/`. The bucket below
# is provisioned just for function source archives — tiny, ~10 KB per zip.

resource "google_storage_bucket" "key_rotator_src" {
  name                        = "oj-key-rotator-src-${var.project_id}"
  location                    = upper(var.primary_region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}

data "archive_file" "rotate_jwt_key_src" {
  type        = "zip"
  output_path = "${path.module}/.terraform-rotate-jwt-key.zip"
  source_dir  = "${path.module}/../key-rotation/rotate_jwt_key"
}

resource "google_storage_bucket_object" "rotate_jwt_key_src" {
  name   = "rotate-jwt-key-${data.archive_file.rotate_jwt_key_src.output_md5}.zip"
  bucket = google_storage_bucket.key_rotator_src.name
  source = data.archive_file.rotate_jwt_key_src.output_path
}

resource "google_cloudfunctions2_function" "rotate_jwt_key" {
  name        = "oj-rotate-jwt-key"
  location    = var.primary_region
  description = "Monthly rotation of the JWT signing secret. Writes a new oj-jwt-keys version."

  build_config {
    runtime     = "python311"
    entry_point = "rotate"
    source {
      storage_source {
        bucket = google_storage_bucket.key_rotator_src.name
        object = google_storage_bucket_object.rotate_jwt_key_src.name
      }
    }
  }

  service_config {
    available_memory      = "256M"
    timeout_seconds       = 60
    service_account_email = google_service_account.key_rotator_sa.email
    environment_variables = {
      PROJECT_ID = var.project_id
      SECRET_ID  = google_secret_manager_secret.jwt_keys.secret_id
    }
  }
}

# ---------- Cloud Function source: oj-rotate-signer-key --------------------

data "archive_file" "rotate_signer_key_src" {
  type        = "zip"
  output_path = "${path.module}/.terraform-rotate-signer-key.zip"
  source_dir  = "${path.module}/../key-rotation/rotate_signer_key"
}

resource "google_storage_bucket_object" "rotate_signer_key_src" {
  name   = "rotate-signer-key-${data.archive_file.rotate_signer_key_src.output_md5}.zip"
  bucket = google_storage_bucket.key_rotator_src.name
  source = data.archive_file.rotate_signer_key_src.output_path
}

resource "google_cloudfunctions2_function" "rotate_signer_key" {
  name        = "oj-rotate-signer-key"
  location    = var.primary_region
  description = "Monthly rotation of the GCS V4 signer SA key. Writes a new oj-problem-signer-key version."

  build_config {
    runtime     = "python311"
    entry_point = "rotate"
    source {
      storage_source {
        bucket = google_storage_bucket.key_rotator_src.name
        object = google_storage_bucket_object.rotate_signer_key_src.name
      }
    }
  }

  service_config {
    available_memory      = "256M"
    timeout_seconds       = 60
    service_account_email = google_service_account.key_rotator_sa.email
    environment_variables = {
      PROJECT_ID      = var.project_id
      SIGNER_SA_EMAIL = google_service_account.problem_service_signer.email
      SECRET_ID       = google_secret_manager_secret.problem_service_signer_key.secret_id
    }
  }
}

# ---------- Scheduler SA → Cloud Function invoker permission ---------------
# The existing `scheduler_sa` only has compute.instanceAdmin (for the auto-
# shutdown jobs); add the cloudfunctions.invoker role on the two rotation
# functions so the Scheduler can POST to their trigger URLs.

resource "google_cloud_run_v2_service_iam_member" "scheduler_invokes_jwt_rotator" {
  # Gen-2 Cloud Functions are backed by Cloud Run v2 under the hood; the IAM
  # binding for "can invoke" lives on the underlying Cloud Run v2 service.
  # The Cloud Run service name == the Cloud Function name on Gen-2.
  location = google_cloudfunctions2_function.rotate_jwt_key.location
  name     = google_cloudfunctions2_function.rotate_jwt_key.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invokes_signer_rotator" {
  location = google_cloudfunctions2_function.rotate_signer_key.location
  name     = google_cloudfunctions2_function.rotate_signer_key.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# ---------- Cloud Scheduler jobs -------------------------------------------

resource "google_cloud_scheduler_job" "key_rotation_jwt" {
  name        = "oj-key-rotation"
  description = "Monthly JWT signing-key rotation (writes a new oj-jwt-keys Secret Manager version)."
  schedule    = var.key_rotation_jwt_cron
  time_zone   = "Etc/UTC"
  region      = var.primary_region

  retry_config {
    retry_count = 3
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
  }

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.rotate_jwt_key.service_config[0].uri

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
      audience              = google_cloudfunctions2_function.rotate_jwt_key.service_config[0].uri
    }
  }
}

resource "google_cloud_scheduler_job" "key_rotation_signer" {
  name        = "oj-key-rotation-signer"
  description = "Monthly signer-SA-key rotation (writes a new oj-problem-signer-key Secret Manager version)."
  schedule    = var.key_rotation_signer_cron
  time_zone   = "Etc/UTC"
  region      = var.primary_region

  retry_config {
    retry_count = 3
    min_backoff_duration = "60s"
    max_backoff_duration = "300s"
  }

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.rotate_signer_key.service_config[0].uri

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
      audience              = google_cloudfunctions2_function.rotate_signer_key.service_config[0].uri
    }
  }
}
