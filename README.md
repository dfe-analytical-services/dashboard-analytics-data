# dashboard-analytics-data
Pipeline to import dashboard analytics data from Google Analytics (GA4) into Unity Catalog for the dashboard analytics app.

# Summary of repo

This repo contains R notebooks and helper scripts that:

* pull GA4 property, daily sessions, and custom event data into raw tables
* transform raw custom events into app-ready event classes
* combine hand-maintained dashboard metadata from `data/app-reference.csv` with GA property metadata for the app reference table

Target catalog: `catalog_40_copper_statistics_services`

## Description of repo files

**params.R**

The purpose of this script is to define shared configuration used across notebooks.

Key items in the script:

* `catalog`, `schema_raw`, and `auth_file`
* a Posit Package Manager CRAN mirror setting
* `auth_json_path`, built from `/Volumes/<catalog>/<schema_raw>/<auth_file>` except on Windows where it is set to `NULL`
* `Sys.info()` output for runtime diagnostics

**utils.R**

The purpose of this script is to provide common helper functions and configuration used across notebooks in the dashboard analytics data project.

The following functions and constants are present in the script:

* Connection helpers:
  * `is_databricks()` detects whether the code is running on Databricks by checking `DATABRICKS_RUNTIME_VERSION`.
  * `connect_databricks()` establishes a database connection using either `sparklyr` (on-cluster) or the Databricks ODBC driver (local), defaulting to the `catalog_40_copper_statistics_services` catalog.
* Package management:
  * `mirror_date` and `options()` pin the CRAN mirror to a specific Posit Package Manager snapshot for Ubuntu focal, with a fallback to standard CRAN.
  * `install_if_needed()` installs one or more packages via `pak` only if they are not already present.
* Paths and constants:
  * `auth_path` is a hardcoded Unity Catalog volume path to the Google service account JSON credential used for GA4 authentication.
* Date helpers:
  * `create_dates()` returns a named list of date offsets relative to a supplied latest date.
* Change tracking and logging:
  * `print_changes_summary()` compares a newly fetched local R data frame against a previous snapshot and reports new row count and new dates.
  * `sdf_print_changes_summary()` is the equivalent helper for `sparklyr` Spark DataFrames.

**data/app-reference.csv**

This CSV is the hand-maintained reference file for dashboard metadata. It includes fields such as `property_id`, `application_type`, `status`, `group`, `team`, `publishing_location`, and repo metadata. `app_dashboard_reference` uses it to build the app-facing reference table and validates that every property in `ga4_dashboard_properties` exists in the CSV.

**raw_dashboard_properties**

The purpose of this notebook is to perform a full refresh of the GA4 property list in `catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_dashboard_properties`.

Flow:

1. Anchors the project root with `here::i_am()`, sources `params.R`, installs missing packages with `pak`, and loads libraries.
2. Sources `utils.R`, authenticates with `ga_auth(json_file = auth_json_path)`, fetches the GA4 account list with `ga_account_list(type = "ga4")`, filters out the "Explore Education Statistics" account, and overwrites the target table.

Notes:

* This notebook is a full-refresh overwrite, not an incremental load.
* The filtered properties table is used by `app_dashboard_reference`, but the daily and custom event raw notebooks still fetch properties directly from `ga_account_list()`.

**raw_dashboard_daily**

The purpose of this notebook is to perform incremental daily ingestion of GA4 dashboard metrics into `catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_daily`.

Here is how each cell functions:

1. Anchors the project root, sources `params.R`, installs missing packages if needed, loads them, sources `utils.R`, and sets `table_name`.
2. Authenticates with `ga_auth()`, using the service account path on Databricks and interactive OAuth locally.
3. Connects via `connect_databricks()`, checks `MAX(date)` in the target table, defaults to `2022-02-02` if empty, sets `changes_since` and `changes_to`, validates both dates, and exits early if the table is already up to date.
4. Loads the existing table into `previous_data`, defines `ga_daily_dashboard_a()` to fetch one property's metrics for the incremental range, fetches properties from `ga_account_list(type = "ga4")`, and builds `latest_data`.
5. Checks that column names match before merging.
6. Row-binds `previous_data` and `latest_data` into `updated_data`, sorts descending by date, and drops `NA` values.
7. Runs four `testthat` assertions: row count increased, no duplicate rows, latest date equals `changes_to`, and no missing values.
8. Writes `updated_data` to a `_temp` table, validates the write, replaces the production table, prints a change summary, and clears the R environment.

Dependencies:

* `R/params.R`
* `R/utils.R`
* External packages: `googleAnalyticsR`, `googleAuthR`, `DBI`, `odbc`, `testthat`, `lubridate`, `arrow`, `here`, `sparklyr`, `dplyr`

**raw_dashboard_custom_event**

The purpose of this notebook is to perform incremental daily ingestion of GA4 custom event data into `catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_custom_event`.

Key differences from `raw_dashboard_daily`:

* the metric is `eventCount`
* the dimensions are `date`, `customEvent:event_category`, and `customEvent:event_label`
* rows are filtered out where `event_category == "(not set)"` or `event_label` is `"(not set)"` or empty

The notebook also fetches properties directly from `ga_account_list(type = "ga4")` rather than from `ga4_dashboard_properties`.

**app_dashboard_reference**

The purpose of this notebook is to create the app reference table `catalog_40_copper_statistics_services.dashboard_analytics_app.dashboard_property_reference` by joining `data/app-reference.csv` to `catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_dashboard_properties`.

Flow:

1. Installs and loads dependencies.
2. Reads `data/app-reference.csv`, coerces `property_id` to character, and removes the CSV `property_name` column.
3. Reads distinct `property_id` and `property_name` from `ga4_dashboard_properties`.
4. Fails if any property in `ga4_dashboard_properties` is missing from the CSV.
5. Builds the final reference table, validates duplicate IDs and missing property names, and overwrites `dashboard_property_reference`.

**app_dashboard_custom_event**

The purpose of this notebook is to create `catalog_40_copper_statistics_services.dashboard_analytics_app.dashboard_custom_events` from `catalog_40_copper_statistics_services.dashboard_analytics_raw.ga4_raw_dashboard_custom_event`.

Key behaviour:

* supports incremental processing or full refresh
* uses a Databricks `full_refresh` widget, or local `FULL_REFRESH`, to control reprocessing
* classifies each event into `Top level navigation`, `Mid level navigation`, `Geography`, or `Other`
* uses `dfeR` geography names to identify geography-related labels
* writes through a `_temp` table before replacing the production table