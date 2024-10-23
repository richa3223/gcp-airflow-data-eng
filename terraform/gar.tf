# Create a Google Artifact Registry for Dataflow Worker custom container image

resource "google_artifact_registry_repository" "mm_df_gar" {
  location      = var.region
  project       = var.project_id
  repository_id = "mm-df-images"
  description   = "MM Dataflow GAR"
  format        = "DOCKER"

  docker_config {
    immutable_tags = true
  }
}

resource "google_artifact_registry_repository_iam_member" "df_worker_access" {
  project    = var.project_id
  location   = google_artifact_registry_repository.mm_df_gar.location
  repository = google_artifact_registry_repository.mm_df_gar.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.df_worker.email}"
}

resource "google_artifact_registry_repository_iam_member" "owner_access" {
  project    = var.project_id
  location   = google_artifact_registry_repository.mm_df_gar.location
  repository = google_artifact_registry_repository.mm_df_gar.name
  role       = "roles/artifactregistry.writer"
  member     = "group:gcp-data-eng-users@bjsscloud.net"
}
