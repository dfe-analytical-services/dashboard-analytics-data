# dashboard-analytics-data

Pipeline to import dashboard analytics data from Google Analytics 4 (GA4) into Unity Catalog for the [dashboard-analytics-devops](https://dev.azure.com/dfe-gov-uk/stats-development/_git/dashboard-analytics-devops) R Shiny app.

## Summary

This repo contains R notebooks and helper scripts that:

* pull GA4 property metadata, daily session metrics, and custom event data into raw tables
* transform raw custom events into app-ready event classes
* combine hand-maintained dashboard metadata (`data/app-reference.csv`) with GA property metadata for the app reference table

All tables are written to the `catalog_40_copper_statistics_services` catalog across two schemas:

| Schema | Table | Populated by |
| --- | --- | --- |
| `dashboard_analytics_raw` | `ga4_dashboard_properties` | `raw_dashboard_properties` |
| `dashboard_analytics_raw` | `ga4_raw_dashboard_daily` | `raw_dashboard_daily` |
| `dashboard_analytics_raw` | `ga4_raw_dashboard_custom_event` | `raw_dashboard_custom_event` |
| `dashboard_analytics_app` | `dashboard_property_reference` | `app_dashboard_reference` |
| `dashboard_analytics_app` | `dashboard_custom_events` | `app_dashboard_custom_event` |

## Repo structure

```
dashboard-analytics-data/
├── data/
│   └── app-reference.csv          # Hand-maintained dashboard metadata
├── R/
│   ├── params.R                   # Shared configuration (catalog, schema, auth path)
│   ├── utils.R                    # Helper functions (connection, packages, dates, logging)
│   ├── raw_dashboard_properties   # Full refresh of GA4 property list
│   ├── raw_dashboard_daily        # Incremental daily session metrics
│   ├── raw_dashboard_custom_event # Incremental custom event ingestion
│   ├── app_dashboard_reference    # Builds app reference table from CSV + properties
│   └── app_dashboard_custom_event # Classifies custom events into event classes
├── LICENSE                        # GPL-3.0
└── README.md
```

## Prerequisites

* **R** (version compatible with the Databricks Runtime on the cluster)
* **Databricks cluster** with an R-capable runtime (e.g. Runtime 15.4)
* **GA4 service-account JSON** stored at `/Volumes/catalog_40_copper_statistics_services/dashboard_analytics_raw/ga_auth/dfe-dashboard-analytics-fd61afbed470.json`
* **R packages** — each notebook installs its own missing packages on first run. Core dependencies: `googleAnalyticsR`, `googleAuthR`, `DBI`, `sparklyr`, `dplyr`, `testthat`, `here`, `dfeR`

For local development:

* [Databricks ODBC driver](https://docs.databricks.com/en/integrations/odbc/index.html)
* Environment variables `DATABRICKS_HOST` and `DATABRICKS_CLUSTER_PATH`
* GA4 authentication falls back to interactive OAuth when no service-account JSON is available

## Getting started

1. Clone this repo from [GitHub](https://github.com/dfe-analytical-services/dashboard-analytics-data).
2. Open the repo as a Databricks Git folder, or work locally with the ODBC driver configured.
3. To test changes, run the affected notebooks manually in the dependency order described below. Production runs are handled by the scheduled Databricks job.

## How to run

### Scheduled pipeline

The notebooks run automatically through the **Dashboard analytics data pipeline** Databricks job:

* **Schedule:** daily at 03:15 UTC
* **Source:** the `main` branch of the Git repo, so changes take effect once merged to `main`
* **Compute:** a shared existing cluster
* **Notifications:** email on success, failure, and when a run exceeds one hour
* **Concurrency:** one run at a time; further triggers are queued

The job's tasks and their dependencies:

| Task | Notebook | Depends on |
| --- | --- | --- |
| `retrieve_ga4_properties` | `R/raw_dashboard_properties` | None |
| `retrieve_ga4_reference_data` | `R/app_dashboard_reference` | `retrieve_ga4_properties` |
| `retrieve_ga4_daily_stats` | `R/raw_dashboard_daily` | `retrieve_ga4_properties` |
| `retrieve_ga4_custom_events` | `R/raw_dashboard_custom_event` | `retrieve_ga4_properties` |
| `add_custom_event_class_column` | `R/app_dashboard_custom_event` | `retrieve_ga4_custom_events` |

After `retrieve_ga4_properties` finishes, the reference, daily, and custom event branches run in parallel. A task only runs if its upstream task succeeds.

### Running manually

To test changes or backfill outside the schedule, run the notebooks in the same dependency order: `raw_dashboard_properties` first, then any of the downstream notebooks, running `raw_dashboard_custom_event` before `app_dashboard_custom_event`.

### Load behaviour

Incremental notebooks fetch from the latest stored date + 1 through `Sys.Date() - 2` (a two-day lag lets GA4 data settle). On a first run with an empty table they backfill from `2022-02-02`.

To reprocess all custom events after changing event-class definitions, run `app_dashboard_custom_event` manually with the `full_refresh` widget set to `TRUE` on Databricks, or set `FULL_REFRESH=TRUE` in your environment locally. The scheduled job runs with the default `FALSE`.

## Detailed file descriptions

### `R/params.R`

Shared configuration sourced by every notebook:

* `catalog`, `schema_raw`, and `auth_file`
* A Posit Package Manager CRAN mirror
* `auth_json_path`, built from `/Volumes/<catalog>/<schema_raw>/<auth_file>` (set to `NULL` on Windows)
* `Sys.info()` output for runtime diagnostics

### `R/utils.R`

Common helper functions and constants:

* **Connection helpers:** `is_databricks()` detects the runtime environment; `connect_databricks()` returns a `sparklyr` connection on-cluster or a DBI/ODBC connection locally.
* **Package management:** `install_if_needed()` installs packages via `pak` only if missing. `mirror_date` and `options()` pin the CRAN snapshot.
* **Paths:** `auth_path` — hardcoded volume path to the GA service-account JSON.
* **Date helpers:** `create_dates()` returns a named list of reference date offsets.
* **Logging:** `print_changes_summary()` reports new rows and dates after a write. `sdf_print_changes_summary()` is the Spark DataFrame equivalent.

### `data/app-reference.csv`

Hand-maintained dashboard metadata. Fields include `property_id`, `application_type`, `status`, `group`, `team`, `publishing_location`, and repo details. When a new GA4 property appears, add a row here before running `app_dashboard_reference`.

### `R/raw_dashboard_properties`

Full-refresh overwrite of `dashboard_analytics_raw.ga4_dashboard_properties`. Fetches the GA4 account list via `ga_account_list(type = "ga4")`, filters out the "Explore Education Statistics" account, and writes the result. Used downstream by `app_dashboard_reference`.

### `R/raw_dashboard_daily`

Incremental ingestion of daily metrics (`totalUsers`, `screenPageViews`, `sessions`) per property into `dashboard_analytics_raw.ga4_raw_dashboard_daily`. Authenticates, checks the latest stored date, fetches the delta from the GA4 API for every property, appends to the existing data, runs `testthat` integrity checks (row count, duplicates, latest date, missing values), and writes through a `_temp` table before replacing the production table.

### `R/raw_dashboard_custom_event`

Incremental ingestion of custom event data into `dashboard_analytics_raw.ga4_raw_dashboard_custom_event`. Same incremental and write pattern as `raw_dashboard_daily`, but fetches `eventCount` by `date`, `event_category`, and `event_label`, and filters out `"(not set)"` / empty labels.

### `R/app_dashboard_reference`

Builds `dashboard_analytics_app.dashboard_property_reference` by joining `data/app-reference.csv` to `ga4_dashboard_properties`. Validates that every GA4 property exists in the CSV (fails if not), checks for duplicate IDs and missing property names, then overwrites the target table.

### `R/app_dashboard_custom_event`

Creates `dashboard_analytics_app.dashboard_custom_events` from the raw custom event table. Supports incremental or full-refresh mode. Classifies each event into one of four classes:

* **Top level navigation** — navbar clicks, service navigation
* **Mid level navigation** — tab panel clicks, domain selection, accordion interactions
* **Geography** — area selections matching region, LA, or LAD names from `dfeR`
* **Other** — everything else

Writes through a `_temp` table with `testthat` validation before replacing production.

## Contributing

This repo is maintained by the Statistics Services Unit in DfE. To contribute:

1. Create a feature branch from `main`.
2. Make your changes and test by running the affected notebooks.
3. If you add a new GA4 property, add a corresponding row to `data/app-reference.csv`.
4. Submit a pull request on GitHub.

## Where to get help

Raise an issue or question in the [GitHub repository](https://github.com/dfe-analytical-services/dashboard-analytics-data/issues).

## Related

* [dashboard-analytics-devops](https://dev.azure.com/dfe-gov-uk/stats-development/_git/dashboard-analytics-devops) — the R Shiny app that consumes these tables

## Licence

This project is licensed under the [GNU General Public License v3.0](LICENSE).