catalog <- "catalog_40_copper_statistics_services"
schema_raw <- "dashboard_analytics_raw"
auth_file <- "ga_auth/dfe-dashboard-analytics-fd61afbed470.json"

print(Sys.info())
if (Sys.info()["sysname"] == "Windows") {
  auth_json_path = NULL
} else {
  auth_json_path <- paste0(
    "/Volumes/",
    catalog,
    "/",
    schema_raw,
    "/ga_auth/",
    auth_file
  )
}
