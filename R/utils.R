is_databricks <- function() {
  nzchar(Sys.getenv("DATABRICKS_RUNTIME_VERSION"))
}

connect_databricks <- function(
  catalog = "catalog_40_copper_statistics_services"
) {
  if (is_databricks()) {
    message("Running on Databricks: using sparklyr")

    sc <- sparklyr::spark_connect(method = "databricks")
    return(sc)
  } else {
    message("Running locally: using Databricks ODBC")

    con <- DBI::dbConnect(
      odbc::databricks(),
      driver = "Databricks ODBC Driver",
      workspace = Sys.getenv("DATABRICKS_HOST"),
      httpPath = Sys.getenv("DATABRICKS_CLUSTER_PATH"),
      catalog = catalog,
      useNativeQuery = FALSE
    )
    return(con)
  }
}


# focal seems to be the ubuntu that my current cluster is running (runtime 15.4)
mirror_date <- "14/02/2025" # can use to freeze versions of dependencies
options(
  repos = c(
    # Get prepackaged binaries for speed where possible
    linuxBinaries = paste0(
      "https://packagemanager.posit.co/cran/__linux__/focal/",
      mirror_date
    ),
    fallbackCRAN = "https://cloud.r-project.org"
  )
)

# Function to use pak to install packages that aren't already installed
install_if_needed <- function(pkg) {
  if (!requireNamespace("pak", quietly = TRUE)) {
    install.packages("pak")
  }

  ## Handle vectors
  if (length(pkg) == 1) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      pak::pkg_install(pkg, ask = FALSE)
    } else {
      message("Skipping install... ", pkg, " already installed")
    }
  } else {
    to_install <- pkg[!sapply(pkg, requireNamespace, quietly = TRUE)]
    if (length(to_install) > 0) {
      pak::pkg_install(to_install, ask = FALSE)
    } else {
      message("Skipping install... all packages already installed")
    }
  }
}

auth_path <- "/Volumes/catalog_40_copper_statistics_services/dashboard_analytics_raw/ga_auth/dfe-dashboard-analytics-fd61afbed470.json"

create_dates <- function(latest_date) {
  list(
    latest_date = latest_date,
    week_date = latest_date - 7,
    four_week_date = latest_date - 28,
    since_4thsep_date = "2024-09-02",
    six_month_date = latest_date - 183,
    one_year_date = latest_date - 365,
    search_console_date = "2023-11-08",
    ga4_date = "2023-06-22",
    all_time_date = "2020-04-03"
  )
}

print_changes_summary <- function(new_table, old_table) {
  if (is.null(old_table)) {
    message("New table summary...")
    message("Number of rows: ", nrow(new_table))
    message("Column names: ", paste(names(new_table), collapse = ", "))
  } else {
    new_dates <- setdiff(
      as.character(new_table$date),
      as.character(old_table$date)
    )
    new_rows <- nrow(as.data.frame(new_table)) - nrow(as.data.frame(old_table))

    message("Updated table summary...")
    message("New rows: ", new_rows)
    message("New dates: ", paste(new_dates, collapse = ","))
    message("Total rows: ", nrow(new_table), " rows")
    message("Column names: ", paste(names(new_table), collapse = ", "))
  }
}

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

LOG_TABLE <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.pipeline_run_log"

# Creates the log table if it does not already exist. Call once per notebook
# run, immediately after connect_databricks().
setup_log_table <- function(conn) {
  sql <- paste(
    "CREATE TABLE IF NOT EXISTS", LOG_TABLE, "(",
    "  run_id           STRING    COMMENT 'Unique identifier for a notebook run',",
    "  run_timestamp    TIMESTAMP COMMENT 'When the log event was recorded',",
    "  notebook_name    STRING    COMMENT 'Name of the notebook',",
    "  status           STRING    COMMENT 'started | success | warning | error',",
    "  message_text     STRING    COMMENT 'Human-readable event description',",
    "  rows_added       INT       COMMENT 'Rows added to target table',",
    "  date_from        STRING    COMMENT 'Start of date range processed',",
    "  date_to          STRING    COMMENT 'End of date range processed',",
    "  properties_total INT       COMMENT 'Total GA4 properties queried',",
    "  properties_failed INT      COMMENT 'GA4 properties that failed',",
    "  error_detail     STRING    COMMENT 'Full error message on failure',",
    "  duration_seconds DOUBLE    COMMENT 'Execution duration in seconds'",
    ") USING DELTA",
    "COMMENT 'Structured log of pipeline run events for dashboard_analytics_data'"
  )
  tryCatch(
    DBI::dbExecute(conn, sql),
    error = function(e) message("WARNING: Could not create log table: ", conditionMessage(e))
  )
}

# Writes one structured log entry to the pipeline_run_log table.
# Wrapped in tryCatch so a logging failure never breaks the pipeline.
log_run_event <- function(
  conn,
  run_id,
  notebook_name,
  status,
  message_text     = NA_character_,
  rows_added       = NA_integer_,
  date_from        = NA_character_,
  date_to          = NA_character_,
  properties_total = NA_integer_,
  properties_failed = NA_integer_,
  error_detail     = NA_character_,
  duration_seconds = NA_real_
) {
  log_entry <- data.frame(
    run_id            = as.character(run_id),
    run_timestamp     = Sys.time(),
    notebook_name     = as.character(notebook_name),
    status            = as.character(status),
    message_text      = as.character(message_text),
    rows_added        = as.integer(rows_added),
    date_from         = as.character(date_from),
    date_to           = as.character(date_to),
    properties_total  = as.integer(properties_total),
    properties_failed = as.integer(properties_failed),
    error_detail      = as.character(error_detail),
    duration_seconds  = as.double(duration_seconds),
    stringsAsFactors  = FALSE
  )
  tryCatch({
    if (is_databricks()) {
      log_df <- sparklyr::copy_to(conn, log_entry, overwrite = TRUE)
      sparklyr::spark_write_table(log_df, LOG_TABLE, mode = "append")
    } else {
      DBI::dbWriteTable(conn, LOG_TABLE, log_entry, append = TRUE)
    }
    message("LOG [", status, "] ", notebook_name, ": ", message_text)
  }, error = function(e) {
    message("WARNING: Failed to write log entry: ", conditionMessage(e))
  })
}

# Temporary second function for notebooks making more use of sdf dataframes
# Eventually should migrate all notebooks to match raw_search_console and use this, removing original function above
sdf_print_changes_summary <- function(new_table, old_table) {
  if (is.null(old_table)) {
    message("New table summary...")
    message("Number of rows: ", sdf_nrow(new_table))
    message("Column names: ", paste(colnames(new_table), collapse = ", "))
  } else {
    new_dates <- as.Date(
      setdiff(
        temp_table_data %>% sdf_distinct("date") %>% sdf_read_column("date"),
        previous_data %>% sdf_distinct("date") %>% sdf_read_column("date")
      )
    )
    new_rows <- sdf_nrow(new_table) - sdf_nrow(old_table)

    message("Updated table summary...")
    message("New rows: ", new_rows)
    message("New dates: ", paste(new_dates, collapse = ","))
    message("Total rows: ", sdf_nrow(new_table), " rows")
    message("Column names: ", paste(colnames(new_table), collapse = ", "))
  }
}
