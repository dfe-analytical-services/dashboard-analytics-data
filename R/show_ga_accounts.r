# Databricks notebook source
# DBTITLE 1,Install and load dependencies
# COMMAND ----------

if(system.file(package='pak')=="") {install.packages("pak")}
packages <- c("googleAnalyticsR", "dplyr", "DBI")
pak::pak(setdiff(packages, rownames(installed.packages())))
lapply(packages, library, character.only = TRUE)


# COMMAND ----------

source("R/utils.R")
source("R/params.R")

if(!is.null(auth_json_path)){message("Auth json path set to: ", auth_json_path)} 
googleAnalyticsR::ga_auth(json_file = auth_json_path)
account_list <- googleAnalyticsR::ga_account_list(type = "ga4")

print(account_list)

conn <- connect_databricks()

dbWriteTable(conn, paste0("ga4_dashboard_properties"), account_list, overwrite = TRUE)

