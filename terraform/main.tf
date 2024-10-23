# Enable necessary APIs

resource "google_project_service" "enabled_apis" {
  for_each                   = toset(var.required_apis)
  service                    = each.key
  project                    = var.project_id
  disable_dependent_services = true
}

