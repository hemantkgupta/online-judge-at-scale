# =============================================================================
# Useful outputs after `tofu apply`. Run `tofu output` to print them.
# =============================================================================

output "region_a_internal_ip" {
  description = "Internal IP of oj-region-a. Use as CRDB --join / Kafka bootstrap from outside this region."
  value       = google_compute_address.region_internal["a"].address
}

output "region_b_internal_ip" {
  description = "Internal IP of oj-region-b."
  value       = google_compute_address.region_internal["b"].address
}

output "artifact_registry_url" {
  description = "Prefix to tag Docker images with for `docker push`."
  value       = "${google_artifact_registry_repository.oj_images.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.oj_images.name}"
}

output "ssh_region_a" {
  description = "Ready-to-paste command to SSH into oj-region-a (via IAP, no public IP needed)."
  value       = "gcloud compute ssh ${var.ssh_user}@${google_compute_instance.oj_region_a.name} --zone=${var.primary_zone} --tunnel-through-iap"
}

output "ssh_region_b" {
  description = "Ready-to-paste command to SSH into oj-region-b."
  value       = "gcloud compute ssh ${var.ssh_user}@${google_compute_instance.oj_region_b.name} --zone=${var.secondary_zone} --tunnel-through-iap"
}

output "start_region_a" {
  description = "Command to start oj-region-a (provisioned in stopped state)."
  value       = "gcloud compute instances start ${google_compute_instance.oj_region_a.name} --zone=${var.primary_zone}"
}

output "start_region_b" {
  description = "Command to start oj-region-b."
  value       = "gcloud compute instances start ${google_compute_instance.oj_region_b.name} --zone=${var.secondary_zone}"
}

output "stop_all" {
  description = "Command to stop both region VMs."
  value       = "gcloud compute instances stop ${google_compute_instance.oj_region_a.name} --zone=${var.primary_zone} && gcloud compute instances stop ${google_compute_instance.oj_region_b.name} --zone=${var.secondary_zone}"
}

output "rotate_jwt_key_now" {
  description = "Manually trigger an out-of-cycle JWT key rotation. Used for emergency rotations after a leak (see docs/design-docs/key-rotation.md#rollback)."
  value       = "gcloud functions call ${google_cloudfunctions2_function.rotate_jwt_key.name} --region=${var.primary_region}"
}

output "rotate_signer_key_now" {
  description = "Manually trigger an out-of-cycle GCS signer-SA key rotation."
  value       = "gcloud functions call ${google_cloudfunctions2_function.rotate_signer_key.name} --region=${var.primary_region}"
}
