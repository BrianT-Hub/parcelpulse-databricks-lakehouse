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

# Mask functions: CARRIER_NAME, CARRIER_CODE, SERVICE_LEVEL
function_statements = [
    f"""
    CREATE OR REPLACE FUNCTION
        {catalog}.ops.mask_carrier_name(value STRING)
    RETURNS STRING
    RETURN CASE
        WHEN value IS NULL THEN NULL
        ELSE CONCAT(
            'CARRIER_',
            UPPER(SUBSTRING(SHA2(value, 256), 1, 10))
        )
    END
    """,

    f"""
    CREATE OR REPLACE FUNCTION
        {catalog}.ops.mask_carrier_code(value STRING)
    RETURNS STRING
    RETURN CASE
        WHEN value IS NULL THEN NULL
        ELSE CONCAT(
            'CODE_',
            UPPER(SUBSTRING(SHA2(value, 256), 1, 10))
        )
    END
    """,

    f"""
    CREATE OR REPLACE FUNCTION
        {catalog}.ops.mask_service_level(value STRING)
    RETURNS STRING
    RETURN CASE
        WHEN value IS NULL THEN NULL
        ELSE CONCAT(
            'SERVICE_',
            UPPER(SUBSTRING(SHA2(value, 256), 1, 10))
        )
    END
    """,
]

for statement in function_statements:
    spark.sql(statement)

print(f"Created masking functions in {catalog}.ops")


# COMMAND ----------

# Apply masks on tables: fact_parcel_performance, monthly_carrier_performance
mask_bindings = [
    (
        f"{catalog}.gold.fact_parcel_performance",
        "CARRIER_NAME",
        f"{catalog}.ops.mask_carrier_name",
    ),
    (
        f"{catalog}.gold.fact_parcel_performance",
        "CARRIER_CODE",
        f"{catalog}.ops.mask_carrier_code",
    ),
    (
        f"{catalog}.gold.fact_parcel_performance",
        "MAPPED_CARRIER",
        f"{catalog}.ops.mask_carrier_name",
    ),
    (
        f"{catalog}.gold.fact_parcel_performance",
        "SERVICE_LEVEL",
        f"{catalog}.ops.mask_service_level",
    ),
    (
        f"{catalog}.gold.monthly_carrier_performance",
        "CARRIER_NAME",
        f"{catalog}.ops.mask_carrier_name",
    ),
    (
        f"{catalog}.gold.monthly_carrier_performance",
        "CARRIER_CODE",
        f"{catalog}.ops.mask_carrier_code",
    ),
    (
        f"{catalog}.gold.monthly_carrier_performance",
        "SERVICE_LEVEL",
        f"{catalog}.ops.mask_service_level",
    ),
]

for table_name, column_name, function_name in mask_bindings:
    spark.sql(
        f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {column_name}
        SET MASK {function_name}
        """
    )

    print(f"Masked {table_name}.{column_name}")

# COMMAND ----------

# Validate masks
mask_metadata = spark.sql(
    f"""
    SELECT
        TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        MASK_NAME
    FROM {catalog}.information_schema.column_masks
    WHERE TABLE_SCHEMA = 'gold'
      AND TABLE_NAME IN (
          'fact_parcel_performance',
          'monthly_carrier_performance'
      )
    ORDER BY TABLE_NAME, COLUMN_NAME
    """
)

display(mask_metadata)

mask_count = mask_metadata.count()

if mask_count != 7:
    raise RuntimeError(
        f"Expected 7 column masks, found {mask_count}"
    )

masked_sample = spark.sql(
    f"""
    SELECT DISTINCT
        CARRIER_CODE,
        CARRIER_NAME,
        MAPPED_CARRIER,
        SERVICE_LEVEL
    FROM {catalog}.gold.fact_parcel_performance
    LIMIT 20
    """
)

display(masked_sample)

print(f"GOVERNANCE VALIDATION PASSED: {mask_count}/7 masks")