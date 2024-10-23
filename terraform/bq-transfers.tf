# Add BigQuery Data Transfer Service job to ingest PKRD Deeside files from GCS into BigQuery

# resource "google_bigquery_data_transfer_config" "pkrd_deeside_ingest" {
#   project                = var.project_id
#   location               = var.region
#   display_name           = "pkrd-deeside-ingest"
#   data_source_id         = "google_cloud_storage"
#   destination_dataset_id = google_bigquery_dataset.mm_internal.dataset_id
#   service_account_name   = google_service_account.bq_dts_sa.email
#   schedule               = "every 15 minutes from 07:00 to 19:00"

#   params = {
#     data_path_template              = "gs://mm-sample-data-57647c46/mm-fin-rec/dts-demo/pkrd_deeside/FB - Finance - Sales Order Line View (SO Status Report)*.csv"
#     destination_table_name_template = google_bigquery_table.pkrd_deeside_ingest.table_id
#     write_disposition               = "MIRROR"
#     skip_leading_rows               = 1
#     file_format                     = "CSV"
#     max_bad_records                 = 0
#   }
# }
