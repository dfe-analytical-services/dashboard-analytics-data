# Databricks notebook source
# DBTITLE 1,Install and load dependencies
if (system.file(package = 'pak') == "") {
  install.packages("pak")
}
packages <- c("dplyr", "DBI", "here", "sparklyr")
missing_packages <- setdiff(packages, rownames(installed.packages()))
if (length(missing_packages)) {
  pak::pkg_install(missing_packages, ask = FALSE)
} else {
  message("All packages already installed")
}
lapply(packages, library, character.only = TRUE)

# COMMAND ----------

# DBTITLE 1,Read app-reference.csv and write to ga4_app_reference table
here::i_am("R/raw_app_reference.r")
source(here::here("R/utils.R"))

app_reference <- read.csv(
  here::here("data/app-reference.csv"),
  stringsAsFactors = FALSE
) |>
  dplyr::mutate(property_id = as.character(property_id))

message("Loaded ", nrow(app_reference), " rows from data/app-reference.csv")
print(app_reference)

conn <- connect_databricks()

# COMMAND ----------

ga_properties_table <- "catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_dashboard_properties"

ga_properties <- if (is_databricks()) {
  sparklyr::sdf_sql(
    conn,
    paste("SELECT DISTINCT CAST(propertyId AS STRING) AS property_id, property_name FROM", ga_properties_table)
  ) |>
    dplyr::collect()
} else {
  DBI::dbGetQuery(
    conn,
    paste("SELECT DISTINCT CAST(propertyId AS STRING) AS property_id, property_name FROM", ga_properties_table)
  )
}

missing_properties <- dplyr::anti_join(
  ga_properties,
  app_reference,
  by = "property_id"
)

if (nrow(missing_properties) > 0) {
  message("\nERROR: The following property IDs in ga4_dashboard_properties are missing from data/app-reference.csv:")
  message("These properties need adding to data/app-reference.csv:\n")
  print(missing_properties |> dplyr::select(property_id, property_name))
  stop("Validation failed: ", nrow(missing_properties), " property ID(s) from ga4_dashboard_properties are missing from app-reference.csv. See above for details.")
} else {
  message("Validation passed: all ", nrow(ga_properties), " property IDs in ga4_dashboard_properties are present in app-reference.csv")
}

# COMMAND ----------

display(ga_properties)

# COMMAND ----------

# DBTITLE 1,Write to dashboard_property_reference
reference_table <- app_reference |>
  dplyr::left_join(
    ga_properties |> dplyr::select(property_id, property_name),
    by = "property_id"
  ) |>
  dplyr::relocate(property_name, .after = property_id)

# Validate: check for duplicate property IDs
duplicate_ids <- reference_table |>
  dplyr::filter(duplicated(property_id) | duplicated(property_id, fromLast = TRUE)) |>
  dplyr::pull(property_id)

if (length(duplicate_ids) > 0) {
  stop(
    "Validation failed: ", length(unique(duplicate_ids)), " duplicate property_id(s) found in reference table:\n",
    paste0("  - ", unique(duplicate_ids), collapse = "\n")
  )
}

# Validate: check for missing property names (property in CSV not found in ga4_dashboard_properties)
missing_names <- reference_table |>
  dplyr::filter(is.na(property_name)) |>
  dplyr::pull(property_id)

if (length(missing_names) > 0) {
  stop(
    "Validation failed: ", length(missing_names), " property_id(s) in app-reference.csv have no matching property_name in ga4_dashboard_properties:\n",
    paste0("  - ", missing_names, collapse = "\n")
  )
}

message("Validation passed: no duplicate property IDs, no missing property names")

output_table <- "catalog_40_copper_statistics_services.dashboard_analytics_app.dashboard_property_reference"

if (is_databricks()) {
  ref_sdf <- copy_to(conn, reference_table, overwrite = TRUE)
  sparklyr::spark_write_table(ref_sdf, output_table, mode = "overwrite")
} else {
  DBI::dbWriteTable(
    conn,
    DBI::Id(schema = "dashboard_analytics_app", table = "dashboard_property_reference"),
    reference_table,
    overwrite = TRUE
  )
}

message("Written ", nrow(reference_table), " rows to ", output_table)
