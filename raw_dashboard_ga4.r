# Databricks notebook source
# DBTITLE 1,Install and load dependencies
source("utils.R")

packages <- c(
  "googleAnalyticsR",
  "googleAuthR",
  "sparklyr",
  "DBI",
  "testthat",
  "lubridate",
  "arrow"
)

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

table_name <- "catalog_40_copper_statistics_services.dashboard_analytics_ga4_raw_dashboard_daily"

con <- DBI::dbConnect(
  odbc::databricks(),
  httpPath = Sys.getenv("DATABRICKS_CLUSTER_PATH"),
  driver = "Databricks ODBC Driver",
  catalog = "catalog_40_copper_statistics_services",
  useNativeQuery = FALSE #required for dbWriteTable to work
  )

# COMMAND ----------

# DBTITLE 1,Authenticate
ga_auth(json = auth_path)

# COMMAND ----------

# DBTITLE 1,Check for latest date from existing data

dbExecute(con, paste(
  "CREATE TABLE IF NOT EXISTS",
  table_name,
  "(date DATE, users DOUBLE, pageviews DOUBLE, sessions DOUBLE)"
))

last_date <- sparklyr::sdf_sql(sc, paste("SELECT MAX(date) FROM", table_name)) %>%
  collect() %>%
  dplyr::pull() %>%
  as.character()

if (is.na(last_date)) {
  # Before tracking started so gets the whole series that is available
  last_date <- "2022-02-02"
}

reference_dates <- create_dates(Sys.Date() - 2) # doing this to make sure the data is complete when we request it

changes_since <- as.Date(last_date) + 1
changes_to <- as.Date(reference_dates$latest_date)

test_that("Query dates are valid", {
  expect_true(is.Date(changes_since))
  expect_true(grepl("\\d{4}-\\d{2}-\\d{2}", as.character(changes_since)))
  expect_true(is.Date(changes_to))
  expect_true(grepl("\\d{4}-\\d{2}-\\d{2}", as.character(changes_to)))

  if (changes_to < changes_since) {
    # Exit the notebook early
    dbutils.notebook.exit("Data is up to date, skipping the rest of the notebook")
  }
})

# COMMAND ----------

# DBTITLE 1,Pull in data
previous_data <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", table_name)) %>% collect()

ga_daily_dashboard_a <- function(property_id, changes_since){
message(property_id)
  ga_data(
        property_id,
        date_range = c(changes_since, changes_to),
        metrics = c("totalUsers", "screenPageViews", "sessions"),
        dimensions = "date",
        limit = -1
      ) |>
    dplyr::mutate(property_id = property_id) |>
    dplyr::rename("pageviews" = screenPageViews) |>
    dplyr::rename("users" = totalUsers)
    }

account_list <- ga_account_list(type = "ga4") |>
  dplyr::filter(!(propertyId %in% c(465816446,509854306)))

result_list <- lapply(account_list$propertyId, ga_daily_dashboard_a, changes_since = changes_since)

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
ga4_spark_df <- copy_to(con, updated_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(ga4_spark_df, paste0(table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", table_name, "_temp")) %>% collect()

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(updated_data))
})

# Replace the old table with the new one
dbExecute(con, paste0("DROP TABLE IF EXISTS ", table_name))
dbExecute(con, paste0("ALTER TABLE ", table_name, "_temp RENAME TO ", table_name))

print_changes_summary(temp_table_data, previous_data)
