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
  "dplyr"
)

missing_packages <- setdiff(packages, rownames(installed.packages()))
if (length(missing_packages)) {
  install.packages(missing_packages)
} else {
  message("All packages already installed")
}
lapply(packages, library, character.only = TRUE)

here::i_am("R/raw_dashboard_properties.r")
source(here("R/utils.R"))
source(here::here("R/params.R"))


table_name <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_custom_event"

# COMMAND ----------

# DBTITLE 1,Authenticate
if (is_databricks()) {
  ga_auth(json = auth_path)
} else {
  ga_auth()
}

# COMMAND ----------

# DBTITLE 1,Check for latest date from existing data
conn <- connect_databricks()

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

reference_dates <- data.frame(
  latest_date = as.Date(Sys.Date() - 2), # doing this to make sure the data is complete when we request it
  stringsAsFactors = FALSE
)

changes_since <- as.Date(last_date) + 1
changes_to <- as.Date(reference_dates$latest_date)

test_that("Query dates are valid", {
  expect_true(is.Date(changes_since))
  expect_true(grepl("\\d{4}-\\d{2}-\\d{2}", as.character(changes_since)))
  expect_true(is.Date(changes_to))
  expect_true(grepl("\\d{4}-\\d{2}-\\d{2}", as.character(changes_to)))

  if (changes_to < changes_since) {
    # Exit the notebook early
    dbutils.notebook.exit(
      "Data is up to date, skipping the rest of the notebook"
    )
  }
})

# COMMAND ----------

# DBTITLE 1,Pull in data
previous_data <- (if (is_databricks()) {
  sparklyr::sdf_sql(conn, paste("SELECT * FROM", table_name)) %>% collect()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT * FROM ", table_name))
})

ga_custom_event <- function(property_id, changes_since) {
  message(property_id)
  tryCatch(
    ga_data(
      property_id,
      date_range = c(changes_since, changes_to),
      metrics = c("eventCount"),
      dimensions = c("date", "customEvent:event_category", "customEvent:event_label"),
      limit = -1
    ) |>
      dplyr::mutate(property_id = property_id) |>
      dplyr::relocate(property_id, .before = date) |>
      dplyr::rename(
        event_category = `customEvent:event_category`,
        event_label = `customEvent:event_label`
      ) |>
      dplyr::filter(event_category != "(not set)" & !(event_label %in% c("(not set)", ""))),
    error = function(e) {
      data.frame(
        property_id = NA,
        date = NA,
        event_category = NA,
        event_label = NA,
        eventCount = NA
      )
    }
  )
}

account_list <- ga_account_list(type = "ga4")

result_list <- lapply(
  account_list$propertyId,
  ga_custom_event,
  changes_since = changes_since
)

latest_data <- dplyr::bind_rows(result_list) |>
  dplyr::arrange(desc(date)) |>
  tidyr::drop_na()

# COMMAND ----------

# DBTITLE 1,Cell 5
if (nrow(latest_data) > 0) {
  display(latest_data)
} else {
  message("No data returned - all GA4 API calls failed or returned NA rows.")
}

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
test_that("New data has more rows than previous data", {
  expect_true(nrow(updated_data) > nrow(previous_data))
})

test_that("New data has no duplicate rows", {
  expect_true(nrow(updated_data) == nrow(dplyr::distinct(updated_data)))
})

test_that("Latest date is as expected", {
  expect_equal(updated_data$date[1], changes_to)
})

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

# Replace the old table with the new one
dbExecute(conn, paste0("DROP TABLE IF EXISTS ", table_name))
dbExecute(
  conn,
  paste0("ALTER TABLE ", table_name, "_temp RENAME TO ", table_name)
)

print_changes_summary(temp_table_data, previous_data)

# Clear out the rubbish
rm(list = ls())
gc()
