variable "gcs_name_prefix" {
  type        = string
  description = "Name prefix of GCS bucket for Terraform remote state"
  default     = "mm-sample-data"
}

variable "org_id" {
  type        = string
  description = "Organization ID"
}

variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "Google Cloud region for resources"
  default     = "europe-west2"
}

variable "required_apis" {
  type        = list(string)
  description = "List of GCP APIs to enable"
  default = [
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
    "cloudapis.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "datastudio.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "storage.googleapis.com",
    "storagetransfer.googleapis.com",
  ]
}

variable "subnet_name_prefix" {
  type        = string
  description = "Name prefix of subnet"
  default     = "mm-eur-west2"
}

variable "vpc_name_prefix" {
  type        = string
  description = "Name prefix of VPC"
  default     = "mm-data-eng"
}
