# Databricks notebook source
# DBTITLE 1,Install and load dependencies
here::i_am("R/raw_dashboard_properties.r")
source(here::here("R/params.R"))

packages <- c(
  "googleAnalyticsR",
  "googleAuthR",
  "DBI",
  "odbc",
  "testthat",
  "lubridate",
  "arrow",
  "here",
  "sparklyr",
  "dplyr"
)

missing_packages <- setdiff(packages, rownames(installed.packages()))
if (length(missing_packages)) {
  message(
    "Installing missing packages: ",
    paste(missing_packages, collapse = ", ")
  )
  pak::pkg_install(missing_packages, ask = FALSE)
} else {
  message("All packages already installed")
}
lapply(packages, library, character.only = TRUE)

source(here("R/utils.R"))


table_name <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_daily"

run_id <- paste0("daily_", format(Sys.time(), "%Y%m%d_%H%M%S"))
start_time <- Sys.time()

# COMMAND ----------

# DBTITLE 1,Authenticate
if (is_databricks()) {
  ga_auth(json = auth_path)
} else {
  ga_auth()
}

# COMMAND ----------

# DBTITLE 1,Check for latest date from existing data
sql_create_table <- paste(
  "CREATE TABLE IF NOT EXISTS",
  table_name,
  "(date DATE, users DOUBLE, pageviews DOUBLE, sessions DOUBLE)"
)

conn <- connect_databricks()
setup_log_table(conn)
log_run_event(
  conn,
  run_id,
  "raw_dashboard_daily",
  "started",
  message_text = "Notebook started"
)

DBI::dbExecute(conn, sql_create_table)

last_date <- if (is_databricks()) {
  sparklyr::sdf_sql(conn, paste("SELECT MAX(date) FROM", table_name)) %>%
    dplyr::collect() %>%
    dplyr::pull() %>%
    as.character()
} else {
  DBI::dbGetQuery(
    conn,
    paste0("SELECT MAX(date) AS last_date FROM ", table_name)
  ) %>%
    dplyr::collect() %>%
    dplyr::pull() %>%
    as.character()
}

if (is.na(last_date)) {
  # Before tracking started so gets the whole series that is available
  last_date <- "2022-02-02"
}

create_dates <- function(run_date = Sys.Date()) {
  data.frame(
    latest_date = as.Date(run_date),
    stringsAsFactors = FALSE
  )
}

reference_dates <- create_dates(Sys.Date() - 2) # doing this to make sure the data is complete when we request it

changes_since <- as.Date(last_date) + 1
changes_to <- as.Date(reference_dates$latest_date)

# Validate dates - critical checks, stop if invalid
if (!is.Date(changes_since) || !is.Date(changes_to)) {
  stop("Invalid date range: changes_since or changes_to is not a valid date")
}

# Early exit if data is already current
if (changes_to < changes_since) {
  stop(
    "Data is already up to date (latest ingested: ",
    as.character(changes_since - 1),
    "). No new records to process."
  )
}

# COMMAND ----------

# DBTITLE 1,Pull in data
previous_data <- (if (is_databricks()) {
  sparklyr::sdf_sql(conn, paste("SELECT * FROM", table_name)) %>% collect()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT * FROM ", table_name))
})

ga_daily_dashboard_a <- function(property_id, changes_since) {
  message(property_id)
  tryCatch(
    ga_data(
      property_id,
      date_range = c(changes_since, changes_to),
      metrics = c("totalUsers", "screenPageViews", "sessions"),
      dimensions = "date",
      limit = -1
    ) |>
      dplyr::mutate(property_id = property_id) |>
      dplyr::relocate(property_id, .before = date),
    error = function(e) {
      message(
        "WARNING: Failed to fetch data for property ",
        property_id,
        ": ",
        conditionMessage(e)
      )
      data.frame(
        property_id = NA,
        date = NA,
        totalUsers = NA,
        screenPageViews = NA,
        sessions = NA
      )
    }
  )
}

account_list <- ga_account_list(type = "ga4")

result_list <- lapply(
  account_list$propertyId,
  ga_daily_dashboard_a,
  changes_since = changes_since
)

failed_properties <- sum(sapply(result_list, function(x) {
  any(is.na(x$property_id))
}))
total_properties <- length(account_list$propertyId)

if (failed_properties > 0) {
  message(
    "WARNING: ",
    failed_properties,
    " of ",
    total_properties,
    " properties failed to fetch data and will be excluded."
  )
  log_run_event(
    conn,
    run_id,
    "raw_dashboard_daily",
    "warning",
    message_text = paste(
      failed_properties,
      "of",
      total_properties,
      "properties failed to return data"
    ),
    properties_total = total_properties,
    properties_failed = failed_properties
  )
}

if (failed_properties / total_properties > 0.5) {
  stop(
    "More than 50% of properties (",
    failed_properties,
    "/",
    total_properties,
    ") failed to return data. Stopping to prevent writing incomplete data."
  )
}

latest_data <- dplyr::bind_rows(result_list) |>
  dplyr::arrange(desc(date)) |>
  tidyr::drop_na()

# COMMAND ----------

# DBTITLE 1,Append new data onto old
test_that("Col names match", {
  expect_equal(names(latest_data), names(previous_data))
})

# COMMAND ----------

updated_data <- latest_data

updated_data <- rbind(previous_data, latest_data) |>
  dplyr::arrange(desc(date)) |>
  tidyr::drop_na()

# COMMAND ----------

# DBTITLE 1,Quick data integrity checks
# Non-critical: row count growth check (can legitimately not grow on low-activity days)
if (nrow(updated_data) <= nrow(previous_data)) {
  warning(
    "Row count did not increase after update (previous: ",
    nrow(previous_data),
    ", updated: ",
    nrow(updated_data),
    "). This may be expected on low-activity days."
  )
}

# Critical: no duplicate rows - stop if this fails
test_that("New data has no duplicate rows", {
  expect_true(nrow(updated_data) == nrow(dplyr::distinct(updated_data)))
})

# Non-critical: latest date (GA4 can have processing delays)
if (updated_data$date[1] != changes_to) {
  warning(
    "Latest date in data (",
    as.character(updated_data$date[1]),
    ") does not match expected (",
    as.character(changes_to),
    "). GA4 data may have a processing delay."
  )
}

# Critical: no missing values - stop if this fails
test_that("Data has no missing values", {
  expect_false(any(is.na(updated_data)))
})

# COMMAND ----------

# DBTITLE 1,Write to table
ga4_df <- copy_to(conn, updated_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
if (is_databricks()) {
  spark_write_table(ga4_df, paste0(table_name, "_temp"), mode = "overwrite")
} else {
  dbWriteTable(conn, paste0(table_name, "_temp"), ga4_df, overwrite = TRUE)
}

temp_table_data <- if (is_databricks()) {
  sparklyr::sdf_sql(conn, paste0("SELECT * FROM ", table_name, "_temp")) %>%
    collect()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT * FROM ", table_name, "_temp"))
}

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(updated_data))
})

# Safe table swap: rename existing to backup first so data is recoverable if rename of temp fails
backup_table <- paste0(table_name, "_backup")
dbExecute(conn, paste0("DROP TABLE IF EXISTS ", backup_table))
dbExecute(conn, paste0("ALTER TABLE ", table_name, " RENAME TO ", backup_table))

tryCatch(
  {
    dbExecute(
      conn,
      paste0("ALTER TABLE ", table_name, "_temp RENAME TO ", table_name)
    )
    dbExecute(conn, paste0("DROP TABLE IF EXISTS ", backup_table))
  },
  error = function(e) {
    message(
      "ERROR: Failed to rename temp table. Restoring original data from backup."
    )
    dbExecute(
      conn,
      paste0("ALTER TABLE ", backup_table, " RENAME TO ", table_name)
    )
    stop(
      "Table swap failed and original data was restored. Error: ",
      conditionMessage(e)
    )
  }
)

log_run_event(
  conn,
  run_id,
  "raw_dashboard_daily",
  "success",
  message_text = "Daily data updated successfully",
  rows_added = nrow(updated_data) - nrow(previous_data),
  date_from = as.character(changes_since),
  date_to = as.character(changes_to),
  properties_total = total_properties,
  properties_failed = failed_properties,
  duration_seconds = as.double(difftime(Sys.time(), start_time, units = "secs"))
)

print_changes_summary(temp_table_data, previous_data)

# Clear out the rubbish
rm(list = ls())
gc()
