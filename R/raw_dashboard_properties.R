# Databricks notebook source
# DBTITLE 1,Install and load dependencies
# COMMAND ----------

if (system.file(package = 'pak') == "") {
  install.packages("pak")
}
packages <- c("googleAnalyticsR", "dplyr", "DBI", "here", "sparklyr")
missing_packages <- setdiff(packages, rownames(installed.packages()))
if (length(missing_packages)) {
  pak::pkg_install(missing_packages, ask = FALSE)
} else {
  message("All packages already installed")
}
lapply(packages, library, character.only = TRUE)


# COMMAND ----------
here::i_am("R/raw_dashboard_properties.r")
source(here("R/utils.R"))
source(here::here("R/params.R"))

if (!is.null(auth_json_path)) {
  message("Auth json path set to: ", auth_json_path)
}
googleAnalyticsR::ga_auth(json_file = auth_json_path)
account_list <- googleAnalyticsR::ga_account_list(type = "ga4") |>
  dplyr::filter(account_name != "Explore Education Statistics")

print(account_list)

conn <- connect_databricks()

if (is_databricks()) {
  spark_write_table(
    account_list,
    "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_dashboard_properties",
    mode = "overwrite"
  )
} else {
  dbWriteTable(
    conn,
    Id(schema = "dashboard_analytics_raw", table = "ga4_dashboard_properties"),
    account_list,
    overwrite = TRUE
  )
}
