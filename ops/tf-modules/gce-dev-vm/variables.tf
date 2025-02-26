variable "project_id" {
  description = "Your GCP Project ID"
  type        = string
}

variable "credentials_file" {
  description = "Path to your GCP service account credentials file"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
}

variable "zone" {
  description = "GCP Zone within the region"
  type        = string
}

variable "service_account_id" {
  description = "Service account ID"
  type        = string
  default     = "oso-service-account"
}

variable "service_account_display_name" {
  description = "Service account display name"
  type        = string
  default     = "OSO Service Account"
}

variable "bigquery_admin_role" {
  description = "BigQuery Admin role"
  type        = string
  default     = "roles/bigquery.admin"
}

variable "iam_service_account_admin_role" {
  description = "IAM Service Account Admin role"
  type        = string
  default     = "roles/iam.serviceAccountAdmin"
}

variable "cloud_platform_role" {
  description = "Cloud Platform role"
  type        = string
  default     = "roles/editor"
}

variable "network_name" {
  description = "Name of the network"
  type        = string
  default     = "dev-network"
}

variable "firewall_name" {
  description = "Name of the firewall rule"
  type        = string
  default     = "ssh-firewall"
}

variable "firewall_ports" {
  description = "Ports to open in the firewall rule"
  type        = list(string)
  default     = ["22"]
}

variable "firewall_target_tags" {
  description = "Target tags for the firewall rule"
  type        = list(string)
  default     = ["dev-vm"]
}

variable "firewall_source_ranges" {
  description = "Source ranges for the firewall rule"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "machine_name" {
  description = "Name of the VM instance"
  type        = string
  default     = "oso-polar"
}

variable "machine_type" {
  description = "Machine type for the VM"
  type        = string
  default     = "n2d-custom-8-16384"
}

variable "machine_hostname" {
  description = "Hostname for the VM instance"
  type        = string
  default     = "oso.polar"
}

variable "vm_tags" {
  description = "Tags for the VM instance"
  type        = list(string)
  default     = ["dev-vm"]
}

variable "service_account_scopes" {
  description = "Service account scopes"
  type        = list(string)
  default = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/bigquery",
  ]
}

variable "boot_disk_device_name" {
  description = "Boot disk device name"
  type        = string
  default     = "oso-polar-disk"
}

variable "disk_type" {
  description = "Boot disk type for the VM"
  type        = string
  default     = "pd-standard"
}

variable "disk_size" {
  description = "Boot disk size in GB"
  type        = number
  default     = 64
}

variable "preemptible" {
  description = "Whether the VM is preemptible"
  type        = bool
  default     = false
}

variable "image" {
  description = "OS Image for the VM instance"
  type        = string
  default     = "debian-cloud/debian-11"
}

variable "public_key_path" {
  description = "Path to your public SSH key file"
  type        = string
}

variable "ssh_user" {
  description = "SSH user for the VM instance"
  type        = string
}

variable "startup_script_path" {
  description = "Path to your startup script file"
  type        = string
  default     = "./scripts/startup.sh"
}
