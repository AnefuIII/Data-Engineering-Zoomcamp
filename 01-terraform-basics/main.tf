terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = "./keys/my-creds.json"
  project     = "vendor-finder-app"
  region      = "europe-west3"
}


resource "google_storage_bucket" "auto_expiring_bucket" {
  name          = "vendor-finder-appauto-expiring-bucket"
  location      = "EUROPE-WEST3"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}