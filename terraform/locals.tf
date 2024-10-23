resource "random_id" "rand_id" {
  byte_length = 4
}

locals {
  gcs_bucket_name         = "${var.gcs_name_prefix}-${random_id.rand_id.hex}"
  gcs_staging_bucket_name = "dataflow-staging-${random_id.rand_id.hex}"
  looker_studio_agent     = "service-org-${var.org_id}@gcp-sa-datastudio.iam.gserviceaccount.com"
  subnet_name             = "${var.subnet_name_prefix}-${random_id.rand_id.hex}"
  vpc_name                = "${var.vpc_name_prefix}-${random_id.rand_id.hex}"
}
