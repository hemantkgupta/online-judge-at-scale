# =============================================================================
# Input variables.
#
# Anything that depends on the operator's environment (project ID, SSH keys,
# region) is exposed here. Defaults match the blog-grade target described in
# infra/gcp/README.md: e2-medium control-plane, n2-standard-2 spot compute in
# Mumbai (asia-south1), 25 GB pd-balanced disks, auto-shutdown at 23:00 IST.
# Override locally via terraform.tfvars (gitignored) or -var on the CLI.
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "online-judge-hk"
}

variable "region" {
  description = "GCP region (Mumbai is closest to the operator; us-central1 is cheapest)"
  type        = string
  default     = "asia-south1"
}

variable "zone" {
  description = "GCP zone within the region"
  type        = string
  default     = "asia-south1-a"
}

# ---------- VM sizing -------------------------------------------------------

variable "control_plane_machine_type" {
  description = "Machine type for the control-plane VM (Kafka, CRDB, Redis, api-gateway in docker-compose)"
  type        = string
  default     = "e2-medium" # 2 vCPU / 4 GB, ~$0.034/hr running
}

variable "compute_machine_type" {
  description = "Machine type for the compute VM. MUST support nested virtualization (n1/n2/c2/c3 families)."
  type        = string
  default     = "n2-standard-2" # 2 vCPU / 8 GB, ~$0.107/hr on-demand or ~$0.026/hr spot
}

variable "compute_use_spot" {
  description = "Use spot pricing for the compute VM. Cheaper (~70% off) but preemptible with 30s notice."
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
  description = "Daily auto-shutdown schedule (Asia/Kolkata). Protects against 'forgot to stop the VMs'."
  type        = string
  default     = "0 23 * * *" # 11:00 PM IST
}

variable "auto_shutdown_timezone" {
  description = "IANA timezone for the auto-shutdown schedule."
  type        = string
  default     = "Asia/Kolkata"
}

# ---------- Artifact Registry -----------------------------------------------

variable "artifact_repo_name" {
  description = "Artifact Registry repository name for Docker images."
  type        = string
  default     = "oj-images"
}

# ---------- Sandbox backend selection (compute VM) --------------------------
# Flip these without rebuilding images. The compute VM startup script writes
# them to /opt/oj/.env which docker-compose picks up. After `tofu apply` the
# VM will need a restart (or `systemctl restart oj-compute.service`) for the
# new values to take effect.

variable "sandbox_backend" {
  description = "Which sandbox backend the execution-worker uses: docker or firecracker."
  type        = string
  default     = "docker"
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
