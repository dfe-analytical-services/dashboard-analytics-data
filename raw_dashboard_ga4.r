# Databricks notebook source
# DBTITLE 1,Install and load dependencies
source("utils.R")

is_databricks <- function() {
  nzchar(Sys.getenv("DATABRICKS_RUNTIME_VERSION"))
}

packages <- if (is_databricks()) { c(
  "googleAnalyticsR",
  "googleAuthR",
  "sparklyr",
  "DBI",
  "testthat",
  "lubridate",
  "arrow"
)
} else { c(
  "googleAnalyticsR",
  "googleAuthR",
  "DBI",
  "odbc",
  "testthat",
  "lubridate",
  "arrow"
)
}

install_if_missing <- function(pkgs) {
  missing <- pkgs[!pkgs %in% rownames(installed.packages())]
  if (length(missing)) {
    install.packages(missing, dependencies = TRUE)
  }
}

install_if_missing(packages)
lapply(packages, library, character.only = TRUE)

table_name <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_daily"

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

DBI::dbExecute(conn, sql_create_table)

last_date <- if(is_databricks()) {
  sparklyr::sdf_sql(conn, paste("SELECT MAX(date) FROM", table_name)) %>%
  dplyr::collect() %>%
  dplyr::pull() %>%
  as.character()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT MAX(date) AS last_date FROM ", table_name)) %>%
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
previous_data <- (
  if(is_databricks()) {
    sparklyr::sdf_sql(conn, paste("SELECT * FROM", table_name)) %>% collect()
    } else {
      DBI::dbGetQuery(conn, paste0("SELECT * FROM ", table_name))
    }
  )
  
ga_daily_dashboard_a <- function(property_id, changes_since){
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
      data.frame(property_id = NA, date = NA, totalUsers = NA, screenPageViews = NA, sessions = NA)
    }
    )
    }

account_list <- ga_account_list(type = "ga4")

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
ga4_spark_df <- copy_to(conn, updated_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
if(is_databricks()) {
  spark_write_table(ga4_spark_df, paste0(table_name, "_temp"), mode = "overwrite")
} else {
  dbWriteTable(conn, paste0(table_name, "_temp"), ga4_spark_df, overwrite = TRUE)
}

temp_table_data <- if(is_databricks()) {
  sparklyr::sdf_sql(conn, paste0("SELECT * FROM ", table_name, "_temp")) %>% collect()
} else {
  DBI::dbGetQuery(conn, paste0("SELECT * FROM ", table_name, "_temp"))
}

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(updated_data))
})

# Replace the old table with the new one
dbExecute(conn, paste0("DROP TABLE IF EXISTS ", table_name))
dbExecute(conn, paste0("ALTER TABLE ", table_name, "_temp RENAME TO ", table_name))

print_changes_summary(temp_table_data, previous_data)
