terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project     = var.project_id
  credentials = file(var.credentials_file)
  region      = var.region
  zone        = var.zone
}

resource "google_service_account" "iam_service_account" {
  account_id   = var.service_account_id
  display_name = var.service_account_display_name
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = var.bigquery_admin_role
  member  = "serviceAccount:${google_service_account.iam_service_account.email}"
}

resource "google_project_iam_member" "iam_service_account_admin" {
  project = var.project_id
  role    = var.iam_service_account_admin_role
  member  = "serviceAccount:${google_service_account.iam_service_account.email}"
}

resource "google_project_iam_member" "cloud_platform" {
  project = var.project_id
  role    = var.cloud_platform_role
  member  = "serviceAccount:${google_service_account.iam_service_account.email}"
}

resource "google_compute_network" "dev_network" {
  name = var.network_name
}

resource "google_compute_firewall" "ssh_firewall" {
  name    = var.firewall_name
  network = google_compute_network.dev_network.name

  allow {
    protocol = "tcp"
    ports    = var.firewall_ports
  }

  target_tags   = var.firewall_target_tags
  source_ranges = var.firewall_source_ranges
}

resource "google_compute_instance" "dev_vm" {
  name                      = var.machine_name
  machine_type              = var.machine_type
  hostname                  = var.machine_hostname
  allow_stopping_for_update = true
  tags                      = var.vm_tags
  service_account {
    email  = google_service_account.iam_service_account.email
    scopes = var.service_account_scopes
  }

  boot_disk {
    device_name = var.boot_disk_device_name

    initialize_params {
      image = var.image
      type  = var.disk_type
      size  = var.disk_size
    }
  }

  scheduling {
    preemptible = var.preemptible
  }

  network_interface {
    network = google_compute_network.dev_network.id
    access_config {

    }
  }

  metadata = {
    ssh-keys       = "${var.ssh_user}:${file(var.public_key_path)}"
    startup-script = file(var.startup_script_path)
  }
}
