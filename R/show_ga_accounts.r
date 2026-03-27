# Databricks notebook source
# DBTITLE 1,Install and load dependencies
# COMMAND ----------
install.packages("pak")
pak::pak("googleAnalyticsR")

# COMMAND ----------

source("utils.R")
source("params.R")
googleAnalyticsR::ga_auth()
googleAnalyticsR::ga_account_list(type = "ga4")

