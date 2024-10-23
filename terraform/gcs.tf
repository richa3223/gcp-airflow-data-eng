# Create private GCS bucket for Terraform remote state

resource "google_storage_bucket" "sample_data" {
  project                     = var.project_id
  name                        = local.gcs_bucket_name
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  versioning {
    enabled = false
  }
  force_destroy = true
}

resource "google_storage_bucket" "dataflow_staging" {
  project                     = var.project_id
  name                        = local.gcs_staging_bucket_name
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  force_destroy = true
}
