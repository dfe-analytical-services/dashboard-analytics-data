# Databricks notebook source
# DBTITLE 1,Install and load dependencies
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
  "dplyr",
  "dfeR"
)

missing_packages <- setdiff(packages, rownames(installed.packages()))
if (length(missing_packages)) {
  install.packages(missing_packages)
} else {
  message("All packages already installed")
}
lapply(packages, library, character.only = TRUE)

here::i_am("R/app_dashboard_custom_event.r")
source(here::here("R/utils.R"))
source(here::here("R/params.R"))

raw_custom_events <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_custom_event"
app_custom_events <- "catalog_40_copper_statistics_services.dashboard_analytics_app.dashboard_custom_events"

# Flag to control incremental vs full refresh processing
# Set to TRUE to reprocess all data (e.g. after updating class definitions)
if (is_databricks()) {
  dbutils.widgets.dropdown("full_refresh", "FALSE", c("TRUE", "FALSE"), "Full Refresh")
  full_refresh_flag <- dbutils.widgets.get("full_refresh") == "TRUE"
} else {
  # You'll need to either run Sys.setenv(FULL_REFRESH = "TRUE") on Positron, or add FULL_REFRESH=TRUE to a .Renviron file
  full_refresh_flag <- as.logical(Sys.getenv("FULL_REFRESH", "FALSE"))
}

# COMMAND ----------

# DBTITLE 1,Add event class column to custom events table
conn <- connect_databricks()

# Attempt to retrieve existing data from the target table
previous_data <- tryCatch(
  {
    if (is_databricks()) {
      sparklyr::sdf_sql(conn, paste("SELECT * FROM", app_custom_events)) %>%
        collect()
    } else {
      DBI::dbGetQuery(conn, paste0("SELECT * FROM ", app_custom_events))
    }
  },
  error = function(e) {
    NULL
  }
)

# If the target table doesn't exist yet, force a full refresh
if (is.null(previous_data)) {
  warning(
    "Target table '", app_custom_events, "' does not exist. ",
    "Forcing full refresh mode for initial load."
  )
  full_refresh_flag <- TRUE
  previous_data <- data.frame()
}

# Determine date cutoff for incremental processing
if (!full_refresh_flag && nrow(previous_data) > 0) {
  cutoff_date <- max(as.Date(previous_data$date))
  date_filter <- paste0(" WHERE date > '", cutoff_date, "'")
  message(paste("Incremental mode: processing data after", cutoff_date))
} else {
  date_filter <- ""
  message("Full refresh mode: processing all data")
}

# Retrieve data from the source table (filtered by cutoff date if incremental)
ga4_raw_custom_events <- (if (is_databricks()) {
  sparklyr::sdf_sql(conn, paste0("SELECT * FROM ", raw_custom_events, date_filter)) %>%
    collect()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT * FROM ", raw_custom_events, date_filter))
})

# Validate that the source query returned data before continuing
if (nrow(ga4_raw_custom_events) == 0) {
  stop(
    "No data returned from source table. ",
    if (date_filter != "") paste0("No new records found since cutoff date ", cutoff_date, ".")
    else "Source table appears to be empty."
  )
}

latest_data <- ga4_raw_custom_events |>
  dplyr::arrange(desc(date)) |>
  tidyr::drop_na() |>
  dplyr::mutate(
    event_class = dplyr::case_when(
      event_category == "navbar click" ~ "Top level navigation",
      event_category == "tab panel clicks" ~ "Mid level navigation",
      event_category %in%
        c("Choose Area", "geography") |
        grepl("^geographic_breakdown", event_category) |
        event_label %in%
          c(
            dfeR::fetch_regions()$region_name,
            dfeR::fetch_las()$la_name,
            dfeR::fetch_lads()$lad_name
          ) ~ "Geography",
      TRUE ~ "Other"
    )
  )

# COMMAND ----------

# DBTITLE 1,Combine and validate data
# Only check column names if previous data exists (skip on first run)
if (nrow(previous_data) > 0) {
  test_that("Col names match", {
    expect_equal(names(latest_data), names(previous_data))
  })
}

# Combine new classified data with previous data (incremental) or use all reprocessed data (full refresh)
if (full_refresh_flag) {
  updated_data <- latest_data
} else {
  updated_data <- dplyr::bind_rows(previous_data, latest_data) |>
    dplyr::distinct() |>
    dplyr::arrange(desc(date))
}

# COMMAND ----------

# DBTITLE 1,Quick data integrity checks
test_that("New data has at least as many rows as previous data", {
  expect_true(nrow(updated_data) >= nrow(previous_data))
})

test_that("New data has no duplicate rows", {
  expect_true(nrow(updated_data) == nrow(dplyr::distinct(updated_data)))
})

test_that("Latest date is as expected", {
  expect_equal(as.Date(updated_data$date[1]), as.Date(Sys.Date() - 2))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(updated_data)))
})

# COMMAND ----------

# DBTITLE 1,Write to table
ga4_df <- copy_to(conn, updated_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
if (is_databricks()) {
  spark_write_table(
    ga4_df,
    paste0(app_custom_events, "_temp"),
    mode = "overwrite"
  )
} else {
  dbWriteTable(
    conn,
    paste0(app_custom_events, "_temp"),
    ga4_df,
    overwrite = TRUE
  )
}

temp_table_data <- if (is_databricks()) {
  sparklyr::sdf_sql(
    conn,
    paste0("SELECT * FROM ", app_custom_events, "_temp")
  ) %>%
    collect()
} else {
  DBI::dbGetQuery(
    conn,
    paste0("SELECT * FROM ", app_custom_events, "_temp")
  )
}

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(updated_data))
})

# Replace the old table with the new one
dbExecute(conn, paste0("DROP TABLE IF EXISTS ", app_custom_events))
dbExecute(
  conn,
  paste0(
    "ALTER TABLE ",
    app_custom_events,
    "_temp RENAME TO ",
    app_custom_events
  )
)

print_changes_summary(temp_table_data, previous_data)

# Clear out the rubbish
rm(list = ls())
gc()
