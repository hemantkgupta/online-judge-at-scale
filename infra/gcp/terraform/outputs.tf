# =============================================================================
# Useful outputs after `tofu apply`. Run `tofu output` to print them.
# =============================================================================

output "control_plane_internal_ip" {
  description = "Internal IP of the control-plane VM. Use this from the compute VM (Kafka, CRDB, Redis live here)."
  value       = google_compute_instance.control_plane.network_interface[0].network_ip
}

output "compute_internal_ip" {
  description = "Internal IP of the compute VM."
  value       = google_compute_instance.compute.network_interface[0].network_ip
}

output "artifact_registry_url" {
  description = "Prefix to tag Docker images with for `docker push`."
  value       = "${google_artifact_registry_repository.oj_images.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.oj_images.name}"
}

output "ssh_control_plane" {
  description = "Ready-to-paste command to SSH into the control-plane VM (via IAP, no public IP needed)."
  value       = "gcloud compute ssh ${var.ssh_user}@${google_compute_instance.control_plane.name} --zone=${var.zone} --tunnel-through-iap"
}

output "ssh_compute" {
  description = "Ready-to-paste command to SSH into the compute VM."
  value       = "gcloud compute ssh ${var.ssh_user}@${google_compute_instance.compute.name} --zone=${var.zone} --tunnel-through-iap"
}

output "start_control_plane" {
  description = "Command to start the control-plane VM (it's provisioned in stopped state)."
  value       = "gcloud compute instances start ${google_compute_instance.control_plane.name} --zone=${var.zone}"
}

output "start_compute" {
  description = "Command to start the compute VM."
  value       = "gcloud compute instances start ${google_compute_instance.compute.name} --zone=${var.zone}"
}

output "stop_all" {
  description = "Command to stop both VMs (use the up.sh/down.sh wrappers once Stage 6 lands)."
  value       = "gcloud compute instances stop ${google_compute_instance.control_plane.name} ${google_compute_instance.compute.name} --zone=${var.zone}"
}
