# Custom Dataflow Worker Service Account

resource "google_service_account" "df_worker" {
  project      = var.project_id
  account_id   = "df-worker"
  display_name = "Custom Dataflow Worker"
}

# Add required IAM roles

resource "google_project_iam_member" "df_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.df_worker.email}"
}

resource "google_project_iam_member" "df_worker_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.df_worker.email}"
}

resource "google_storage_bucket_iam_member" "df_worker" {
  bucket = google_storage_bucket.sample_data.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.df_worker.email}"
}

resource "google_storage_bucket_iam_member" "df_worker_staging" {
  bucket = google_storage_bucket.dataflow_staging.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.df_worker.email}"
}

resource "google_bigquery_dataset_access" "df_worker" {
  project       = var.project_id
  dataset_id    = google_bigquery_dataset.mm_internal.dataset_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = google_service_account.df_worker.email
}

# Dataflow Developer Service Account for job deployment

resource "google_service_account" "df_developer" {
  project      = var.project_id
  account_id   = "df-developer"
  display_name = "Dataflow Developer"
}

# Assign IAM roles to Dataflow Developer Service Account

resource "google_project_iam_member" "df_developer" {
  project = var.project_id
  role    = "roles/dataflow.developer"
  member  = "serviceAccount:${google_service_account.df_developer.email}"
}

# Enable service account impersonation on Dataflow Developer Service Account

resource "google_service_account_iam_member" "df_developer" {
  service_account_id = google_service_account.df_developer.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:tim.antrobus.admin@bjsscloud.net"
}

# Looker Studio Read-Only Service Account

resource "google_service_account" "looker_studio_readonly" {
  project      = var.project_id
  account_id   = "looker-studio-readonly"
  display_name = "Looker Studio Read-only"
}

# Add required IAM roles

resource "google_project_iam_member" "looker_studio_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.looker_studio_readonly.email}"
}

resource "google_bigquery_dataset_access" "looker_studio_readonly" {
  project       = var.project_id
  dataset_id    = google_bigquery_dataset.mm_reporting.dataset_id
  role          = "roles/bigquery.dataViewer"
  user_by_email = google_service_account.looker_studio_readonly.email
}

# Enable Looker Studio Service Agent to impersonate Looker Studio read-only account

resource "google_service_account_iam_member" "looker_studio_agent" {
  service_account_id = google_service_account.looker_studio_readonly.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${local.looker_studio_agent}"
}

# BQ Data Transfer Service account 

resource "google_service_account" "bq_dts_sa" {
  project      = var.project_id
  account_id   = "bq-dts"
  display_name = "BigQuery Data Transfer Service account"
}

# IAM permissions for BQ DTS service account

resource "google_storage_bucket_iam_member" "bq_dts_sa" {
  bucket = google_storage_bucket.sample_data.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.bq_dts_sa.email}"
}

resource "google_bigquery_dataset_access" "bq_dts_sa" {
  project       = var.project_id
  dataset_id    = google_bigquery_dataset.mm_internal.dataset_id
  role          = "roles/bigquery.admin"
  user_by_email = google_service_account.bq_dts_sa.email
}
