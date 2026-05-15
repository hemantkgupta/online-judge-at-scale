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
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
