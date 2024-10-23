# Create a BQ dataset for internal use

resource "google_bigquery_dataset" "mm_internal" {
  project                    = var.project_id
  dataset_id                 = "mm_fin_internal"
  friendly_name              = "MM Finance Internal"
  location                   = var.region
  max_time_travel_hours      = 168 # 7 days
  delete_contents_on_destroy = true
  lifecycle {
    ignore_changes = [access]
  }
}

# Create a BQ dataset for reporting use

resource "google_bigquery_dataset" "mm_reporting" {
  project                    = var.project_id
  dataset_id                 = "mm_fin_reporting"
  friendly_name              = "MM Finance Reporting"
  location                   = var.region
  delete_contents_on_destroy = true
  lifecycle {
    ignore_changes = [access]
  }
}

# Make the Reporting dataset an Authorized Dataset for the Internal dataset
# This enables SQL views in the Reporting dataset to read from tables in the Internal dataset

resource "google_bigquery_dataset_access" "authorized" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.mm_internal.dataset_id
  dataset {
    dataset {
      project_id = google_bigquery_dataset.mm_reporting.project
      dataset_id = google_bigquery_dataset.mm_reporting.dataset_id
    }
    target_types = ["VIEWS"]
  }
}

# Daily processed input dataset table

resource "google_bigquery_table" "fin_rec_data" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.mm_internal.dataset_id
  table_id                 = "fin_rec_data"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type  = "MONTH"
    field = "record_date"
  }

  clustering = [
    "depot_category",
    "depot_id",
    "moveorder_short",
    "sku",
  ]
  schema = file("${path.module}/bq-schemas/fin_rec_data.json")
}

# Processed input dataset history table - i.e. cumulative data table model

resource "google_bigquery_table" "fin_rec_data_hist" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.mm_internal.dataset_id
  table_id                 = "fin_rec_data_history"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type  = "MONTH"
    field = "record_date"
  }

  clustering = [
    "depot_category",
    "depot_id",
    "moveorder_short",
    "sku",
  ]
  schema = file("${path.module}/bq-schemas/fin_rec_data_history.json")
}

resource "google_bigquery_table" "fin_rec_variance" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.mm_internal.dataset_id
  table_id                 = "fin_rec_variance"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type  = "MONTH"
    field = "effective_date"
  }

  clustering = [
    "created_ts",
    "effective_date",
    "variance_type",
    "depot_category",
  ]
  schema = file("${path.module}/bq-schemas/fin_rec_variance.json")
}

resource "google_bigquery_table" "fin_rec_summary" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.mm_internal.dataset_id
  table_id                 = "fin_rec_summary"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type  = "MONTH"
    field = "effective_date"
  }

  clustering = [
    "created_ts",
    "effective_date",
    "category"
  ]
  schema = file("${path.module}/bq-schemas/fin_rec_summary.json")
}

# Daily pricing data ingest table

resource "google_bigquery_table" "fin_rec_pricing" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.mm_internal.dataset_id
  table_id                 = "fin_rec_pricing"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type  = "MONTH"
    field = "pricing_date"
  }

  clustering = [
    "created_ts",
    "correlation_id",
    "pricing_date",
    "sku"
  ]
  schema = file("${path.module}/bq-schemas/fin_rec_pricing.json")
}

# Historical pricing data table 

resource "google_bigquery_table" "fin_rec_pricing_hist" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.mm_internal.dataset_id
  table_id                 = "fin_rec_pricing_history"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type  = "MONTH"
    field = "pricing_date"
  }

  clustering = [
    "created_ts",
    "correlation_id",
    "pricing_date",
    "sku"
  ]
  schema = file("${path.module}/bq-schemas/fin_rec_pricing_history.json")
}

# Authorized View on daily pricing table - de-duplicates records resulting from partial or whole intraday updates

resource "google_bigquery_table" "daily_pricing_view" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.mm_reporting.dataset_id
  table_id   = "fin_rec_pricing_daily"

  view {
    query          = file("${path.module}/bq-views/fin_rec_pricing_view.sql")
    use_legacy_sql = false
  }
}

# Authorized View on daily fin_rec_data table - de-duplicates records resulting from partial or whole intraday updates

resource "google_bigquery_table" "daily_finrecdata_view" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.mm_reporting.dataset_id
  table_id   = "fin_rec_data_daily"

  view {
    query          = file("${path.module}/bq-views/fin_rec_data_view.sql")
    use_legacy_sql = false
  }
}

# Table Valued Function enabling dynamic date range querying across fin_rec_data daily and history tables

resource "google_bigquery_routine" "fin_rec_data" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "fin_rec_data_for_dates"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/fin_rec_data_for_dates_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "start_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "end_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }
}

# Table Valued Function as authorized routine on internal dataset

resource "google_bigquery_dataset_access" "authorized_tvf" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.mm_internal.dataset_id
  routine {
    project_id = google_bigquery_routine.fin_rec_data.project
    dataset_id = google_bigquery_dataset.mm_reporting.dataset_id
    routine_id = google_bigquery_routine.fin_rec_data.routine_id
  }
}

# Table Valued Function to fetch pricing data for a user specified date

resource "google_bigquery_routine" "fin_rec_pricing" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "fin_rec_pricing_for_date"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/fin_rec_pricing_for_date_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "price_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }
}

# Add Table Valued Function as authorized routine on internal dataset

resource "google_bigquery_dataset_access" "authorized_pricing_tvf" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.mm_internal.dataset_id
  routine {
    project_id = google_bigquery_routine.fin_rec_pricing.project
    dataset_id = google_bigquery_dataset.mm_reporting.dataset_id
    routine_id = google_bigquery_routine.fin_rec_pricing.routine_id
  }
}

# Table Valued Function to calculate variance by category and moveorder for Looker Studio reporting

resource "google_bigquery_routine" "category_mo_var" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "category_mo_var"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/category_mo_var_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "start_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "end_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "category"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "STRING" })
  }
}

# Table Valued Function to calculate variance by category and sku for Looker Studio reporting

resource "google_bigquery_routine" "category_sku_var" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "category_sku_var"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/category_sku_var_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "start_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "end_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "category"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "STRING" })
  }
}

# Table Valued Function to calculate variance by category, depot and date for Looker Studio reporting

resource "google_bigquery_routine" "category_depot_date_var" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "category_depot_date_var"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/category_depot_date_var_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "start_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "end_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "category"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "STRING" })
  }
}

# Table Valued Function to calculate variance summary totals for Looker Studio reporting

resource "google_bigquery_routine" "category_summary_var" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "category_summary_var"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/category_summary_var_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "start_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "end_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }
}

# Table Valued Function to provide variance by depot and date for visualisation in Looker Studio reporting

resource "google_bigquery_routine" "depot_date_var" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "depot_date_var"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/depot_date_var_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "start_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "end_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }
}

# Table Valued Function to provide variance range tiers by depot for visualisation in Looker Studio reporting

resource "google_bigquery_routine" "depot_var_tiers" {
  project         = var.project_id
  dataset_id      = google_bigquery_dataset.mm_reporting.dataset_id
  routine_id      = "depot_var_tiers"
  routine_type    = "TABLE_VALUED_FUNCTION"
  language        = "SQL"
  definition_body = templatefile("${path.module}/bq-routines/depot_var_tiers_func.tftpl", { proj = var.project_id })

  arguments {
    name          = "start_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }

  arguments {
    name          = "end_date"
    argument_kind = "FIXED_TYPE"
    data_type     = jsonencode({ "typeKind" : "DATE" })
  }
}

# PKRD Deeside ingest table

resource "google_bigquery_table" "pkrd_deeside_ingest" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.mm_internal.dataset_id
  table_id                 = "pkrd_deeside_ingest"
  require_partition_filter = true
  deletion_protection      = false

  time_partitioning {
    type = "DAY"
  }

  schema = file("${path.module}/bq-schemas/pkrd_deeside_load.json")
}
