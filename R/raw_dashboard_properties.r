# Databricks notebook source
# DBTITLE 1,Install and load dependencies
# COMMAND

here::i_am("R/raw_dashboard_properties.r")
source(here::here("R/params.R"))

if (system.file(package = 'pak') == "") {
  install.packages("pak")
}
packages <- c("googleAnalyticsR", "dplyr", "DBI", "here", "sparklyr")
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


# COMMAND ----------

# DBTITLE 1,Auth, fetch properties, write to table
source(here("R/utils.R"))

run_id <- paste0("properties_", format(Sys.time(), "%Y%m%d_%H%M%S"))
start_time <- Sys.time()

conn <- connect_databricks()
setup_log_table(conn)

log_run_event(
  conn,
  run_id,
  "raw_dashboard_properties",
  "started",
  message_text = "Notebook started"
)

tryCatch(
  {
    if (!is.null(auth_json_path)) {
      message("Auth json path set to: ", auth_json_path)
    }
    googleAnalyticsR::ga_auth(json_file = auth_json_path)
    account_list <- googleAnalyticsR::ga_account_list(type = "ga4") |>
      dplyr::filter(account_name != "Explore Education Statistics")

    print(account_list)

    if (is_databricks()) {
      account_list_table <- copy_to(conn, account_list, overwrite = TRUE)
      sparklyr::spark_write_table(
        account_list_table,
        "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_dashboard_properties",
        mode = "overwrite"
      )
    } else {
      dbWriteTable(
        conn,
        Id(
          schema = "dashboard_analytics_raw",
          table = "ga4_dashboard_properties"
        ),
        account_list,
        overwrite = TRUE
      )
    }

    log_run_event(
      conn,
      run_id,
      "raw_dashboard_properties",
      "success",
      message_text = paste(
        "Properties table updated with",
        nrow(account_list),
        "rows"
      ),
      rows_added = nrow(account_list),
      duration_seconds = as.double(difftime(
        Sys.time(),
        start_time,
        units = "secs"
      ))
    )
  },
  error = function(e) {
    log_run_event(
      conn,
      run_id,
      "raw_dashboard_properties",
      "error",
      error_detail = conditionMessage(e),
      duration_seconds = as.double(difftime(
        Sys.time(),
        start_time,
        units = "secs"
      ))
    )
    stop(e)
  }
)

# Clear out the rubbish
rm(list = ls())
gc()
