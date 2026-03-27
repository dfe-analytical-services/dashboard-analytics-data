# Databricks notebook source
# DBTITLE 1,Install and load dependencies
# COMMAND ----------

if(system.file(package='pak')=="") {install.packages("pak")}
packages <- c("googleAnalyticsR", "dplyr")
pak::pak(setdiff(packages, rownames(installed.packages())))

# COMMAND ----------

source("utils.R")
source("params.R")
googleAnalyticsR::ga_auth(json_file = auth_json_path)
account_list <- googleAnalyticsR::ga_account_list(type = "ga4")

print(account_list)
