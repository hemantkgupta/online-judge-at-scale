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
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
