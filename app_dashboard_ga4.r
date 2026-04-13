# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_table_name <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.dashboard_ga4"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.dashboard_pub_scrape"
write_service_table_name <- "catalog_40_copper_statistics_services.analytics_app.dashboard_service"
write_publication_table_name <- "catalog_40_copper_statistics_services.analytics_app.dashboard_publication"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Read in and check table integrity

full_data <- sparklyr::sdf_sql(sc, paste("
  SELECT
   date, users, pageviews, sessions, property_id FROM", ga4_table_name, "
  ORDER BY date DESC
")) %>% collect()

test_that("No duplicate rows", {
  expect_true(nrow(full_data) == nrow(dplyr::distinct(full_data)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(full_data)))
})

dates <- create_dates(max(full_data$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(full_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

full_data <- full_data %>%
  mutate(page_type = case_when(
    str_detect(pagePath, "/data-guidance") ~ "Data guidance",
    str_detect(pagePath, "/prerelease-access-list") ~ "Pre-release access",
    str_detect(pagePath, "/find-statistics/") ~ "Release page",
    str_detect(pagePath, "/find-statistics") ~ "Find stats navigation",
    str_detect(pagePath, "/data-catalogue/data-set") ~ "Data catalogue dataset",
    str_detect(pagePath, "/data-catalogue") ~ "Data catalogue navigation",
    str_detect(pagePath, "/data-tables/permalink") ~ "Permalink",
    str_detect(pagePath, "/data-tables/") ~ "Table tool",
    str_detect(pagePath, "/methodology/") ~ "Methodology page",
    str_detect(pagePath, "/methodology") ~ "Methodology navigation",
    str_detect(pagePath, "/subscriptions/") ~ "Subscriptions",
    str_detect(pagePath, "/glossary") ~ "Glossary",
    str_detect(pagePath, "/cookies") ~ "Cookies",
    str_detect(pagePath, "/") ~ "Homepage",
    str_detect(pagePath, "(other)") ~ "Other",
    TRUE ~ "NA"
  ))

# COMMAND ----------

test_that("There are no events without a page type classification", {
  expect_true(nrow(full_data %>% filter(page_type == "NA")) == 0)
})

# COMMAND ----------

# DBTITLE 1,Filter table down to only publication and release pages
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)

# COMMAND ----------

# DBTITLE 1,Create a slug column and join on publication titles
joined_data <- full_data |>
  mutate(slug = str_remove(pagePath, "^/(methodology|find-statistics|data-tables|data-catalogue)/")) |>
  mutate(slug = str_remove(slug, "-methodology")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_trim(slug, side = "both")) |>
  mutate(slug = str_to_lower(slug)) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title) |>
  mutate(publication = str_to_title(publication))

dates <- create_dates(max(full_data$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(full_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# selecting just the columns we're interested in storing and creating a service level table
dashboard_service <- joined_data %>%
  select(date, users, pageviews, sessions, property_id) %>%
  group_by(date) %>%
  summarise(
    pageviews = sum(pageviews),
    sessions = sum(sessions),
    .groups = "keep"
  )

# COMMAND ----------

# selecting just the columns we're interested in storing and creating a service level table
dashboard_publication <- joined_data %>%
  filter(page_type == "Release page") %>%
  filter(publication != "NA") %>%
  select(date, publication, users, pageviews, sessions, property_id) %>%
  group_by(date, publication, users, property_id) %>%
  summarise(
    pageviews = sum(pageviews),
    sessions = sum(sessions),
    .groups = "keep"
  )

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_service_spark_df <- copy_to(sc, dashboard_service, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_service_spark_df, paste0(write_service_table_name, "_temp"), mode = "overwrite")

temp_service_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_service_table_name, "_temp")) %>% collect()
previous_service_data <- tryCatch(
  {
    sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_service_table_name)) %>% collect()
  },
  error = function(e) {
    NULL
  }
)

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_service_table_data), nrow(dashboard_service))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_service_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_service_table_name, "_temp RENAME TO ", write_service_table_name))

print_changes_summary(temp_service_table_data, previous_service_data)

# COMMAND ----------

updated_publication_spark_df <- copy_to(sc, dashboard_publication, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_publication_spark_df, paste0(write_publication_table_name, "_temp"), mode = "overwrite")

temp_publication_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_publication_table_name, "_temp")) %>% collect()
previous_publication_data <- tryCatch(
  {
    sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_publication_table_name)) %>% collect()
  },
  error = function(e) {
    NULL
  }
)

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_publication_table_data), nrow(dashboard_publication))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_publication_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_publication_table_name, "_temp RENAME TO ", write_publication_table_name))

print_changes_summary(temp_publication_table_data, previous_publication_data)

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE:
# MAGIC
# MAGIC Remember if aggregating up from pagePath level avgtimeonpage and bouncerate will no longer be accurate. To aggregate and have an accurate time on page we'd need to shorten time series to just GA4 data and for bounce rate we'd need to rerun the query at the right level.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Note:**
# MAGIC
# MAGIC We are left with two tables:
# MAGIC
# MAGIC 1. **Publication Table**
# MAGIC    For release pages only.
# MAGIC    - date
# MAGIC    - publication
# MAGIC    - device
# MAGIC    - browser
# MAGIC    - pageviews
# MAGIC    - sessions
# MAGIC
# MAGIC 2. **Service Table**
# MAGIC    For all service pages.
# MAGIC    - date
# MAGIC    - page_type
# MAGIC    - device
# MAGIC    - browser
# MAGIC    - pageviews
# MAGIC    - sessions
