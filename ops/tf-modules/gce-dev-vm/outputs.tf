output "vm_public_ip" {
  description = "Public IP of the VM"
  value       = google_compute_instance.dev_vm.network_interface.0.access_config.0.nat_ip
}

output "service_account_email" {
  description = "Email address of the IAM service account"
  value       = google_service_account.iam_service_account.email
}
