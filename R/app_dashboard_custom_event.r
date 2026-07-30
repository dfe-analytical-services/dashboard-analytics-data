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

source(here("R/utils.R"))
source(here::here("R/params.R"))

custom_event_table <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_custom_event"
custom_event_class_table <- "catalog_40_copper_statistics_services.dashboard_analytics_app.dashboard_custom_events"

# COMMAND ----------

# DBTITLE 1,Add event class column to custom events table
conn <- connect_databricks()

previous_data <- (if (is_databricks()) {
  sparklyr::sdf_sql(conn, paste("SELECT * FROM", custom_event_class_table)) %>%
    collect()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT * FROM ", custom_event_class_table))
})

ga4_raw_custom_events <- (if (is_databricks()) {
  sparklyr::sdf_sql(conn, paste("SELECT * FROM", custom_event_table)) %>%
    collect()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT * FROM ", custom_event_table))
})

latest_data <- ga4_raw_custom_events |>
  dplyr::arrange(desc(date)) |>
  tidyr::drop_na() |>
  dplyr::mutate(
    event_class = dplyr::case_when(
      event_category == "navbar click" ~ "Top Level Navigation",
      event_category == "tap panel clicks" ~ "Mid Level Navigation",
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

test_that("Col names match", {
  expect_equal(names(latest_data), names(previous_data))
})

updated_data <- latest_data

# COMMAND ----------

# DBTITLE 1,Quick data integrity checks
reference_dates <- data.frame(
  latest_date = as.Date(Sys.Date() - 2), # doing this to make sure the data is complete when we request it
  stringsAsFactors = FALSE
)

changes_to <- as.Date(reference_dates$latest_date)

test_that("New data has more rows than previous data", {
  expect_true(nrow(updated_data) > nrow(previous_data))
})

test_that("New data has no duplicate rows", {
  expect_true(nrow(updated_data) == nrow(dplyr::distinct(updated_data)))
})

test_that("Latest date is as expected", {
  expect_equal(as.Date(updated_data$date[1]), changes_to)
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
    paste0(custom_event_class_table, "_temp"),
    mode = "overwrite"
  )
} else {
  dbWriteTable(
    conn,
    paste0(custom_event_class_table, "_temp"),
    ga4_df,
    overwrite = TRUE
  )
}

temp_table_data <- if (is_databricks()) {
  sparklyr::sdf_sql(
    conn,
    paste0("SELECT * FROM ", custom_event_class_table, "_temp")
  ) %>%
    collect()
} else {
  DBI::dbGetQuery(
    conn,
    paste0("SELECT * FROM ", custom_event_class_table, "_temp")
  )
}

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(updated_data))
})

# Replace the old table with the new one
dbExecute(conn, paste0("DROP TABLE IF EXISTS ", custom_event_class_table))
dbExecute(
  conn,
  paste0(
    "ALTER TABLE ",
    custom_event_class_table,
    "_temp RENAME TO ",
    custom_event_class_table
  )
)

print_changes_summary(temp_table_data, previous_data)

# Clear out the rubbish
rm(list = ls())
gc()
