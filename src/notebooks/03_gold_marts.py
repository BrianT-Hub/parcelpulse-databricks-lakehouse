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
        f"Invalid catalog_name: {catalog}."
        f"Allowed values: {sorted(allowed_catalogs)}"
    )

source_table = f"{catalog}.silver.parcel_performance"
fact_table = f"{catalog}.gold.fact_parcel_performance"

print(f"Catalog:    {catalog}")
print(f"Source:     {source_table}")
print(f"Target:     {fact_table}")

# COMMAND ----------

# DBTITLE 1,Cell 2
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fact_table} USING DELTA AS
    SELECT
        -- Fact grain
        PACKAGE_GUID,
        TRACKING_NUMBER,

        -- Dates,
        SHIP_DATE,
        DELIVERY_DATE,
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        CALENDAR_MONTH_NAME,
        CALENDAR_MONTH_SHORT_NAME,
        FISCAL_QUARTER,
        FISCAL_WEEK,

        -- Account
        ACCOUNT_NUMBER,
        COALESCE(MAPPED_BRAND, BRAND, 'UNMAPPED') AS ACCOUNT_BRAND,
        COALESCE(MAPPED_CHANNEL, CHANNEL_OF_DISTRIBUTION, 'UNMAPPED') AS ACCOUNT_CHANNEL,

        -- Carrier and service
        CARRIER_CODE,
        CARRIER_NAME,
        MAPPED_CARRIER,
        SERVICE_DESCRIPTION,
        COALESCE(SERVICE_LEVEL, 'UNMAPPED') AS SERVICE_LEVEL,

        -- Origin and destination
        ORIGIN_STATE,
        ORIGIN_ZIP,
        ORIGIN_ZIP3,
        DESTINATION_STATE,
        DESTINATION_ZIP,
        DESTINATION_ZIP3,

        DC_ID,
        DC_CITY_KEY,
        DC_STATE,

        -- Shipment attributes
        MODE,
        MOVE_TYPE,
        PACKAGE_TYPE,
        PIECES,
        SHIP_WEIGHT,
        BILL_WEIGHT, 
        ZONE,
        TRANSIT_ZONE,

        -- Financial measures
        FREIGHT_CHARGES,
        ACCESSORIAL_CHARGE,
        DISCOUNT_AMOUNT, 
        FUEL_CHARGES,
        DUTY_AND_TAX_CHARGES, 
        NET_CHARGES,
        TOTAL_PAID,

        -- SLA measures
        PROMISED_TRANSIT_DAYS,
        ACTUAL_TRANSIT_DAYS,
        ON_TIME_FLAG,
        DELIVERY_STATUS,
        DAYS_LATE,

        -- Other BI measures 
        1 AS PACKAGE_QTY,

        CASE 
            WHEN ON_TIME_FLAG IS NOT NULL THEN 1 ELSE 0
        END AS MEASURED_PACKAGE_QTY,

        COALESCE(ON_TIME_FLAG, 0) AS ON_TIME_PACKAGE_QTY,

        CASE
            WHEN ON_TIME_FLAG = 0 THEN 1 ELSE 0 
        END AS LATE_PACKAGE_QTY,

        -- Data-quality fields
        LOOKUP_QUALITY_STATUS, 
        MISSING_LOOKUPS,
        ACCOUNT_MAPPING_MATCHED,
        SERVICE_LOOKUP_MATCHED,
        DC_LOOKUP_MATCHED,
        TRANSIT_LOOKUP_MATCHED,
        CALENDAR_LOOKUP_MATCHED,

        -- Lineage
        _source_file_name,
        _source_window_start,
        _source_window_end,
        _ingested_at,
        _silver_processed_at,
        _enriched_at,
        _sla_calculated_at,
        CURRENT_TIMESTAMP() AS _gold_published_at

    FROM {source_table}
    """
)

spark.sql(
    f"""
    ALTER TABLE {fact_table} 
    SET TBLPROPERTIES (
        'parcelpulse.layer' = 'gold',
        'parcelpulse.grain' = 'one row per eligible PACKAGE_GUID'
    )
    """
)

print(f"Created {fact_table}")

# COMMAND ----------

fact_validation = spark.sql(
    f"""
    SELECT
        COUNT(*) AS fact_rows,
        COUNT(DISTINCT PACKAGE_GUID) AS unique_packages,
        COUNT(*) - COUNT(DISTINCT PACKAGE_GUID) AS duplicate_package_rows,
        SUM(PACKAGE_QTY) AS total_package_qty,
        SUM(MEASURED_PACKAGE_QTY) AS measured_package_qty,
        SUM(ON_TIME_PACKAGE_QTY) AS on_time_package_qty,
        SUM(LATE_PACKAGE_QTY) AS late_package_qty,
        SUM(
            CASE WHEN MEASURED_PACKAGE_QTY = 0 THEN 1 ELSE 0 END
        ) AS unmeasured_package_qty,
        ROUND(
            100.0 * SUM(ON_TIME_PACKAGE_QTY) / NULLIF(SUM(MEASURED_PACKAGE_QTY), 0),
            2
        ) AS on_time_pct,
        ROUND(
            SUM(NET_CHARGES), 2) AS total_net_charges

    FROM {fact_table}
    """
)

display(fact_validation)

# COMMAND ----------

monthly_kpi_table = (
    f"{catalog}.gold.monthly_carrier_performance"
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {monthly_kpi_table} USING DELTA AS
    SELECT
        CAST (DATE_TRUNC('MONTH', SHIP_DATE) AS DATE) AS SHIP_MONTH,
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        CALENDAR_MONTH_SHORT_NAME,

        CARRIER_CODE,
        CARRIER_NAME,
        SERVICE_LEVEL,

        -- Volume 
        SUM(PACKAGE_QTY) AS PACKAGE_QTY,
        SUM(MEASURED_PACKAGE_QTY) AS MEASURED_PACKAGE_QTY,
        SUM(ON_TIME_PACKAGE_QTY) AS ON_TIME_PACKAGE_QTY,
        SUM(LATE_PACKAGE_QTY) AS LATE_PACKAGE_QTY,

        SUM(
            CASE WHEN DELIVERY_STATUS = 'NO_DELIVERY_DATE' THEN 1 ELSE 0 END
        ) AS NO_DELIVERY_DATE_QTY,

        SUM(
            CASE WHEN DELIVERY_STATUS = 'MISSING_PROMISE' THEN 1 ELSE 0 END
        ) AS MISSING_PROMISE_QTY,

        -- Service KPIs
        ROUND(
            100.0 * SUM(ON_TIME_PACKAGE_QTY) / NULLIF(SUM(MEASURED_PACKAGE_QTY), 0),
            2
        ) AS ON_TIME_PCT,
        ROUND(
            AVG(ACTUAL_TRANSIT_DAYS),
            2
        ) AS AVG_ACTUAL_TRANSIT_DAYS,
        ROUND(
            AVG(PROMISED_TRANSIT_DAYS),
            2
        ) AS AVG_PROMISED_TRANSIT_DAYS,
        ROUND(
            AVG(DAYS_LATE),
            2
        ) AS AVG_DAYS_LATE,
        PERCENTILE_APPROX(DAYS_LATE, 0.95) AS P95_DAYS_LATE,

        -- Cost KPIs
        ROUND(
            SUM(NET_CHARGES), 
            2
        ) AS TOTAL_NET_CHARGES, 
        ROUND(
            AVG(NET_CHARGES),
            2
        ) AS AVG_NET_CHARGE_PER_PACKAGE,
        ROUND(
            SUM(FUEL_CHARGES),
            2
        ) AS TOTAL_FUEL_CHARGES, 
        ROUND(
            SUM(SHIP_WEIGHT),
            2
        ) AS TOTAL_SHIP_WEIGHT,

        -- Lookup quality
        SUM(
            CASE WHEN LOOKUP_QUALITY_STATUS = 'COMPLETE' THEN 1 ELSE 0 END
        ) AS COMPLETE_LOOKUP_QTY,
        SUM(
            CASE WHEN LOOKUP_QUALITY_STATUS = 'PARTIAL' THEN 1 ELSE 0 END
        ) AS PARTIAL_LOOKUP_QTY,

        CURRENT_TIMESTAMP() AS _gold_published_at
    
    FROM {fact_table}

    GROUP BY 
        CAST(DATE_TRUNC('MONTH', SHIP_DATE) AS DATE),
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        CALENDAR_MONTH_SHORT_NAME,
        CARRIER_CODE,
        CARRIER_NAME,
        SERVICE_LEVEL
    """
)

spark.sql(
    f"""
    ALTER TABLE {monthly_kpi_table}
    SET TBLPROPERTIES(
        'parcelpulse.layer' = 'gold',
        'parcelpulse.grain' = 'one row per ship month, carrier, and service level'
    )
    """
)

print(f"Created {monthly_kpi_table}")

# COMMAND ----------

mart_validation = spark.sql(
    f"""
    WITH 
    fact_totals AS (
        SELECT
            SUM(PACKAGE_QTY) AS fact_packages,
            SUM(MEASURED_PACKAGE_QTY) AS fact_measured, 
            ROUND(SUM(NET_CHARGES), 2) AS fact_charges
        FROM {fact_table}
    ),

    mart_total AS (
        SELECT
            COUNT(*) AS mart_rows,
            SUM(PACKAGE_QTY) AS mart_packages,
            SUM(MEASURED_PACKAGE_QTY) AS mart_measured, 
            ROUND(SUM(TOTAL_NET_CHARGES), 2) AS mart_charges,
            SUM(
                CASE
                    WHEN ON_TIME_PCT < 0 OR ON_TIME_PCT > 100 THEN 1 ELSE 0
                END
            ) AS invalid_rate_rows

        FROM {monthly_kpi_table}
    )

    SELECT 
        m.mart_rows,
        f.fact_packages,
        m.mart_packages,
        m.mart_packages - f.fact_packages AS package_variance, 
        f.fact_measured,
        m.mart_measured,
        m.mart_measured - f.fact_measured AS measured_variance,
        f.fact_charges,
        m.mart_charges,
        m.mart_charges - f.fact_charges AS charge_variance,
        m.invalid_rate_rows

    FROM fact_totals AS f
    CROSS JOIN mart_total AS m 
    """
)

display(mart_validation) 

# COMMAND ----------

# DBTITLE 1,Cell 6
monthly_summary_table = (f"{catalog}.gold.monthly_business_summary")

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {monthly_summary_table} USING DELTA AS
    SELECT
        CAST(DATE_TRUNC('MONTH', SHIP_DATE) AS DATE) AS SHIP_MONTH,
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        CALENDAR_MONTH_SHORT_NAME,

        -- Volume
        SUM(PACKAGE_QTY) AS PACKAGE_QTY,
        SUM(MEASURED_PACKAGE_QTY) AS MEASURED_PACKAGE_QTY,
        SUM(ON_TIME_PACKAGE_QTY) AS ON_TIME_PACKAGE_QTY,
        SUM(LATE_PACKAGE_QTY) AS LATE_PACKAGE_QTY,
        SUM(
            CASE WHEN DELIVERY_STATUS = 'NO_DELIVERY_DATE' THEN 1 ELSE 0 END
        ) AS NO_DELIVERY_DATE_QTY,
        SUM(
            CASE WHEN DELIVERY_STATUS = 'MISSING_PROMISE' THEN 1 ELSE 0 END
        ) AS MISSING_PROMISE_QTY,

        -- Service performance
        ROUND(
            100.0 * SUM(ON_TIME_PACKAGE_QTY) / NULLIF(SUM(MEASURED_PACKAGE_QTY), 0),
            2
        ) AS ON_TIME_PCT,
        ROUND(
            100.0 * SUM(MEASURED_PACKAGE_QTY) / NULLIF(SUM(PACKAGE_QTY), 0),
            2
        ) AS SLA_MEASUREMENT_COVERAGE_PCT,
        ROUND(
            AVG(ACTUAL_TRANSIT_DAYS),
            2
        ) AS AVG_ACTUAL_TRANSIT_DAYS,
        ROUND(
            AVG(PROMISED_TRANSIT_DAYS),
            2
        ) AS AVG_PROMISED_TRANSIT_DAYS,
        ROUND(
            AVG(DAYS_LATE),
            2
        ) AS AVG_DAYS_LATE,
        PERCENTILE_APPROX(DAYS_LATE, 0.95) AS P95_DAYS_LATE,

        -- Cost
        ROUND(
            SUM(NET_CHARGES),
            2
        ) AS TOTAL_NET_CHARGES,
        ROUND(
            AVG(NET_CHARGES),
            2
        ) AS AVG_NET_CHARGE_PER_PACKAGE,
        ROUND(
            SUM(SHIP_WEIGHT),
            2
        ) AS TOTAL_SHIP_WEIGHT,
        ROUND(
            SUM(NET_CHARGES) / NULLIF(SUM(SHIP_WEIGHT), 0),
            2
        ) AS NET_COST_PER_WEIGHT_UNIT,

        -- DATA quality
        ROUND(
            100.0 * SUM(CASE WHEN LOOKUP_QUALITY_STATUS = 'COMPLETE' THEN 1 ELSE 0 END) / NULLIF(SUM(PACKAGE_QTY), 0),
            2
        ) AS COMPLETE_LOOKUP_PCT,

        CURRENT_TIMESTAMP() AS _gold_published_at

    FROM {fact_table}

    GROUP BY
        CAST(DATE_TRUNC('MONTH', SHIP_DATE) AS DATE),
        CALENDAR_YEAR,
        CALENDAR_MONTH_NUMBER,
        CALENDAR_MONTH_SHORT_NAME
    """
)

spark.sql(
    f"""
    ALTER TABLE {monthly_summary_table}
    SET TBLPROPERTIES(
        'parcelpulse.layer' = 'gold',
        'parcelpulse.grain' = 'one row per ship month'
    )
    """
)

print(f"Created {monthly_summary_table}")

# COMMAND ----------

summary_validation = spark.sql(
    f"""
    SELECT
        COUNT(*) AS month_count,
        SUM(PACKAGE_QTY) AS total_packages, 
        SUM(PACKAGE_QTY) - (SELECT SUM(PACKAGE_QTY) FROM {fact_table}) AS package_variance,
        MIN(ON_TIME_PCT) AS minimum_on_time_pct,
        MAX(ON_TIME_PCT) AS maximum_on_time_pct,
        MIN(SLA_MEASUREMENT_COVERAGE_PCT) AS minimum_measurement_coverage_pct,
        MIN(COMPLETE_LOOKUP_PCT) AS minimum_complete_lookup_pct,
        SUM(
            CASE 
                WHEN ON_TIME_PCT < 0 OR ON_TIME_PCT > 100 THEN 1 ELSE 0
            END
        ) AS invalid_on_time_rate_rows
    
    FROM {monthly_summary_table}
    """
)

display(summary_validation)

# COMMAND ----------

