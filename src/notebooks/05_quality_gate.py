# Databricks notebook source
dbutils.widgets.text(
    "catalog_name",
    "parcelpulse_dev",
    "Target catalog"
)

catalog = dbutils.widgets.get("catalog_name").strip().lower()

allowed_catalogs = {
    "parcelpulse_dev",
    "parcelpulse_test",
    "parcelpulse_prod",
}

if catalog not in allowed_catalogs:
    raise ValueError(
        f"Invalid catalog_name: {catalog}. "
        f"Allowed values: {sorted(allowed_catalogs)}"
    )

print(f"Running quality gate for: {catalog}")

quality_metrics = spark.sql(
    f"""
    SELECT
        (SELECT COUNT(*) FROM {catalog}.bronze.parcel_raw) AS bronze_rows,

        (SELECT COUNT(DISTINCT PACKAGE_GUID) FROM {catalog}.bronze.parcel_raw) AS bronze_unique_packages,

        (SELECT COUNT(*) FROM {catalog}.bronze.parcel_raw WHERE _rescued_data IS NOT NULL) AS bronze_rescued_rows,

        (SELECT COUNT(*) FROM {catalog}.silver.parcel_clean) AS silver_clean_rows,

        (SELECT COUNT(DISTINCT PACKAGE_GUID) FROM {catalog}.silver.parcel_clean) AS silver_unique_packages,

        (SELECT COUNT(*) FROM {catalog}.silver.parcel_quarantine) AS quarantine_rows,

        (SELECT COUNT(*) FROM {catalog}.silver.parcel_enriched) AS enriched_rows,

        (SELECT COUNT(DISTINCT PACKAGE_GUID) FROM {catalog}.silver.parcel_enriched) AS enriched_unique_packages,

        (SELECT COUNT(*) FROM {catalog}.gold.fact_parcel_performance) AS fact_rows,

        (SELECT COUNT(DISTINCT PACKAGE_GUID) FROM {catalog}.gold.fact_parcel_performance) AS fact_unique_packages,

        (SELECT 
            COALESCE(SUM(MEASURED_PACKAGE_QTY), 0) 
        FROM {catalog}.gold.fact_parcel_performance) AS fact_measured_rows,

        (SELECT 
            COALESCE(SUM(ON_TIME_PACKAGE_QTY) + SUM(LATE_PACKAGE_QTY), 0)
         FROM {catalog}.gold.fact_parcel_performance) AS fact_status_rows,

        (SELECT 
            COALESCE(SUM(PACKAGE_QTY), 0)
         FROM {catalog}.gold.monthly_carrier_performance) AS carrier_mart_packages,

        (SELECT 
            COALESCE(SUM(PACKAGE_QTY), 0)
         FROM {catalog}.gold.monthly_business_summary) AS summary_mart_packages,

        (SELECT 
            COUNT(*) 
        FROM {catalog}.information_schema.column_masks 
        WHERE TABLE_SCHEMA = 'gold' 
            AND ((TABLE_NAME = 'fact_parcel_performance' 
            AND COLUMN_NAME IN ('CARRIER_NAME', 'CARRIER_CODE', 'MAPPED_CARRIER', 'SERVICE_LEVEL'))
            OR (TABLE_NAME = 'monthly_carrier_performance' AND COLUMN_NAME IN ('CARRIER_NAME', 'CARRIER_CODE', 'SERVICE_LEVEL'))
            )
        ) AS applied_mask_count
    """
).first().asDict()


checks = [
    (
        "bronze_has_data",
        quality_metrics["bronze_rows"] > 0,
        quality_metrics["bronze_rows"],
        "> 0"
    ),
    (
        "bronze_window_overlap_is_valid",
        quality_metrics["bronze_rows"]
        >= quality_metrics["bronze_unique_packages"],
        quality_metrics["bronze_rows"],
        f">= {quality_metrics['bronze_unique_packages']}"
    ),
    (
        "no_rescued_bronze_rows",
        quality_metrics["bronze_rescued_rows"] == 0,
        quality_metrics["bronze_rescued_rows"],
        "0"
    ),
    (
        "silver_preserves_unique_packages",
        quality_metrics["silver_clean_rows"]
        == quality_metrics["bronze_unique_packages"],
        quality_metrics["silver_clean_rows"],
        str(quality_metrics["bronze_unique_packages"])
    ),
    (
        "silver_has_unique_grain",
        quality_metrics["silver_clean_rows"]
        == quality_metrics["silver_unique_packages"],
        quality_metrics["silver_clean_rows"]
        - quality_metrics["silver_unique_packages"],
        "0 duplicate rows"
    ),
    (
        "quarantine_is_empty",
        quality_metrics["quarantine_rows"] == 0,
        quality_metrics["quarantine_rows"],
        "0"
    ),
    (
        "enriched_has_unique_grain",
        quality_metrics["enriched_rows"]
        == quality_metrics["enriched_unique_packages"],
        quality_metrics["enriched_rows"]
        - quality_metrics["enriched_unique_packages"],
        "0 duplicate rows"
    ),
    (
        "gold_matches_enriched",
        quality_metrics["fact_rows"]
        == quality_metrics["enriched_rows"],
        quality_metrics["fact_rows"],
        str(quality_metrics["enriched_rows"])
    ),
    (
        "gold_has_unique_grain",
        quality_metrics["fact_rows"]
        == quality_metrics["fact_unique_packages"],
        quality_metrics["fact_rows"]
        - quality_metrics["fact_unique_packages"],
        "0 duplicate rows"
    ),
    (
        "measured_statuses_balance",
        quality_metrics["fact_measured_rows"]
        == quality_metrics["fact_status_rows"],
        quality_metrics["fact_status_rows"],
        str(quality_metrics["fact_measured_rows"])
    ),
    (
        "carrier_mart_reconciles",
        quality_metrics["carrier_mart_packages"]
        == quality_metrics["fact_rows"],
        quality_metrics["carrier_mart_packages"],
        str(quality_metrics["fact_rows"])
    ),
    (
        "summary_mart_reconciles",
        quality_metrics["summary_mart_packages"]
        == quality_metrics["fact_rows"],
        quality_metrics["summary_mart_packages"],
        str(quality_metrics["fact_rows"])
    ),
    (
        "all_column_masks_applied",
        quality_metrics["applied_mask_count"] == 7,
        quality_metrics["applied_mask_count"],
        "7"
    ),
]

check_results = spark.createDataFrame(
    [
        (
            name,
            bool(passed),
            str(actual),
            expectation
        )
        for name, passed, actual, expectation in checks
    ],
    [
        "check_name",
        "passed",
        "actual",
        "expectation"
    ]
)

display(check_results)

failures = [
    name
    for name, passed, _, _ in checks
    if not passed
]

if failures:
    raise RuntimeError(
        "Pipeline quality gate failed: "
        + ", ".join(failures)
    )

print(f"QUALITY GATE PASSED: {len(checks)}/{len(checks)} checks")