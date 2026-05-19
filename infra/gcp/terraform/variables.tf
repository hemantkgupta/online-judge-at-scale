# =============================================================================
# Input variables.
#
# Multi-region setup: two VMs in two regions, each running the full stack.
# Defaults pick asia-south1 (Mumbai, closest to operator) as primary and
# us-central1 (cheap US region) as secondary. Override via terraform.tfvars.
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "online-judge-hk"
}

# ---------- Region selection ------------------------------------------------

variable "primary_region" {
  description = "Primary GCP region (closest to operator)."
  type        = string
  default     = "asia-south1"
}

variable "primary_zone" {
  description = "Primary GCP zone within primary_region."
  type        = string
  default     = "asia-south1-a"
}

variable "secondary_region" {
  description = "Secondary GCP region (geographically distant for multi-region demo / DR)."
  type        = string
  default     = "us-central1"
}

variable "secondary_zone" {
  description = "Secondary GCP zone within secondary_region."
  type        = string
  default     = "us-central1-a"
}

# ---------- VM sizing -------------------------------------------------------

variable "region_machine_type" {
  description = "Machine type for both region VMs. MUST support nested virtualization (n1/n2/c2/c3) — both VMs run Firecracker."
  type        = string
  default     = "n2-standard-2" # 2 vCPU / 8 GB, ~$0.107/hr on-demand or ~$0.026/hr spot
}

variable "region_use_spot" {
  description = "Use spot pricing for both region VMs. Cheaper (~70% off) but preemptible with 30s notice."
  type        = bool
  default     = true
}

variable "disk_size_gb" {
  description = "Boot disk size in GB for both VMs. pd-balanced ~$0.10/GB/mo."
  type        = number
  default     = 25
}

# ---------- SSH -------------------------------------------------------------

variable "ssh_user" {
  description = "Login user created on the VMs (matches the Pi setup)."
  type        = string
  default     = "hemant"
}

variable "ssh_public_key_path" {
  description = "Path on the operator's machine to the SSH public key to inject into VM metadata."
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

# ---------- Auto-shutdown safety net ----------------------------------------

variable "auto_shutdown_cron" {
  description = "Daily auto-shutdown schedule. Protects against 'forgot to stop the VMs'."
  type        = string
  default     = "0 23 * * *" # 11:00 PM IST
}

variable "auto_shutdown_timezone" {
  description = "IANA timezone for the auto-shutdown schedule."
  type        = string
  default     = "Asia/Kolkata"
}

# ---------- Key rotation ----------------------------------------------------
# Two monthly schedules, deliberately offset by ~14 days so we don't stack
# the JWT and signer-SA rotations on the same night. See
# `infra/gcp/terraform/key-rotation.tf` for the wiring.

variable "key_rotation_jwt_cron" {
  description = "Cron expression for the monthly JWT signing-key rotation. UTC."
  type        = string
  default     = "0 2 1 * *" # 02:00 UTC on the 1st of every month
}

variable "key_rotation_signer_cron" {
  description = "Cron expression for the monthly GCS V4 signer-SA-key rotation. UTC."
  type        = string
  default     = "0 2 15 * *" # 02:00 UTC on the 15th of every month (offset from JWT by 14 days)
}

# ---------- Artifact Registry -----------------------------------------------

variable "artifact_repo_name" {
  description = "Artifact Registry repository name for Docker images. Hosted in primary_region; secondary VM pulls cross-region."
  type        = string
  default     = "oj-images"
}

# ---------- Sandbox backend selection ---------------------------------------
# Flip these without rebuilding images. The startup script writes them to
# /opt/oj/.env which docker-compose picks up. After `tofu apply` the VM
# needs a restart (or `systemctl restart oj-region.service`) for the new
# values to take effect.

variable "sandbox_backend" {
  description = "Which sandbox backend the execution-worker uses: docker or firecracker."
  type        = string
  default     = "firecracker"
  validation {
    condition     = contains(["docker", "firecracker"], var.sandbox_backend)
    error_message = "sandbox_backend must be one of: docker, firecracker."
  }
}

variable "sandbox_docker_runtime" {
  description = "Docker OCI runtime: runc (default Linux) or runsc (gVisor)."
  type        = string
  default     = "runc"
  validation {
    condition     = contains(["runc", "runsc"], var.sandbox_docker_runtime)
    error_message = "sandbox_docker_runtime must be one of: runc, runsc."
  }
}

variable "linux_hardening_enabled" {
  description = "Apply Seccomp-BPF + capability drop + cgroupns to the Docker backend."
  type        = bool
  default     = false
}
