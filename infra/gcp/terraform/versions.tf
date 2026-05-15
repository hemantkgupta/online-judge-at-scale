# =============================================================================
# Provider pins.
#
# OpenTofu 1.6+ is the minimum we need (for sensitive-output handling and
# moved-block support). Google provider 6.x has the modern shape for
# advanced_machine_features (nested virtualization), Cloud Scheduler OIDC,
# and Artifact Registry IAM bindings we use below.
# =============================================================================

terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    # Stage 4 generates the api-gateway JWT secret in-state so it survives
    # tofu destroy + apply cycles. No CSPRNG roundtrip to a KMS needed for
    # a dev-grade setup.
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    # archive_file zips the in-repo `infra/firecracker/agent/` source tree
    # so terraform can inject it into the compute VM's startup-script
    # metadata. `build-rootfs.sh` on the VM unpacks it and runs `go build`
    # to compile the Execution Agent for the guest before packing the
    # rootfs. Avoids a git clone or extra GCS bucket dependency at boot.
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
