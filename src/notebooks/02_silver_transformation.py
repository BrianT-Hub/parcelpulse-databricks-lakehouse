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

source_table = f"{catalog}.bronze.parcel_raw"
target_table = f"{catalog}.silver.parcel_clean"
quarantine_table = f"{catalog}.silver.parcel_quarantine"

print(f"Catalog:       {catalog}")
print(f"Source:        {source_table}")
print(f"Target:        {target_table}")
print(f"Quarantine:    {quarantine_table}")

# COMMAND ----------


source_profile = spark.sql(
    f"""
    SELECT
        COUNT(*) AS landed_rows,
        COUNT(DISTINCT PACKAGE_GUID) AS unique_packages,
        COUNT(*) - COUNT(DISTINCT PACKAGE_GUID) AS overlapping_window_rows,
        SUM(
            CASE
                WHEN PACKAGE_GUID IS NULL OR TRIM(PACKAGE_GUID) = ''
                THEN 1
                ELSE 0
            END      
        ) AS missing_package_guid_rows,
        SUM(
            CASE
                WHEN SHIP_DATE IS NOT NULL AND TRY_CAST(SHIP_DATE AS DATE) IS NULL
                THEN 1
                ELSE 0
            END
        ) AS invalid_ship_date_rows,
        SUM(
            CASE
                WHEN DELIVERY_DATE IS NOT NULL AND TRY_CAST(DELIVERY_DATE AS DATE) IS NULL
                THEN 1
                ELSE 0
            END
        ) AS invalid_delivery_date_rows,
        MIN(TRY_CAST(SHIP_DATE AS DATE)) AS earliest_ship_date,
        MAX(TRY_CAST(SHIP_DATE AS DATE)) AS latest_ship_date
    FROM {source_table}
    """
)

display(source_profile)

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

bronze_df = spark.table(source_table)

#1. trim strings
staged_df = bronze_df

for field in bronze_df.schema.fields:
    if isinstance(field.dataType, StringType):
        staged_df = staged_df.withColumn(
            field.name, 
            F.trim(F.col(field.name))
        )

#2. extract source data window from source name
window_start_text = F.regexp_extract(
    F.col("_source_file_name"),
    r"parcel_synthetic_(\d{4}-\d{2})_to_",
    1
)

window_end_text = F.regexp_extract(
    F.col("_source_file_name"),
    r"_to_(\d{4}-\d{2})\.tsv$",
    1
)

staged_df = (
    staged_df
    .withColumn(
        "_source_window_start",
        F.to_date(F.concat(window_start_text, F.lit("-01")))
    )
    .withColumn(
        "_source_window_end",
        F.last_day(
            F.to_date(F.concat(window_end_text, F.lit("-01")))
        )
    )
)

#3. Safely parse date. Return NULL when fail
staged_df = (
    staged_df
    .withColumn(
        "_parsed_ship_date",
        F.expr("TRY_CAST(SHIP_DATE AS DATE)")
    )
    .withColumn(
        "_parsed_delivery_date",
        F.expr("TRY_CAST(DELIVERY_DATE AS DATE)")
    )
    .withColumn(
        "_parsed_process_date",
        F.expr("TRY_CAST(PROCESS_DATE AS DATE)")
    )
    .withColumn(
        "_parsed_invoice_date",
        F.expr("TRY_CAST(INVOICE_DAY_YEAR AS DATE)")
    )
)

#4. Setup rules for data quality
staged_df = staged_df.withColumn(
    "_dq_reason",
    F.concat_ws(
        "; ",
        F.when(
            F.col("PACKAGE_GUID").isNull() | (F.col("PACKAGE_GUID") == ""),
            F.lit("MISSING_PACKAGE_GUID")
        ),
        F.when(
            F.col("SHIP_DATE").isNull() | (F.col("SHIP_DATE") == ""),
            F.lit("MISSING_SHIP_DATE")
        ),
        F.when(
            F.col("SHIP_DATE").isNotNull() & F.col("_parsed_ship_date").isNull(),
            F.lit("INVALID_SHIP_DATE")
        ),
        F.when(
            F.col("DELIVERY_DATE").isNotNull() & F.col("_parsed_delivery_date").isNull(),
            F.lit("INVALID_DELIEVERY_DATE")
        ),
        F.when(
            F.col("_parsed_delivery_date").isNotNull() & 
            (
                F.col("_parsed_delivery_date") < F.col("_parsed_ship_date")
            ),
            F.lit("DELIVERY_BEFORE_SHIPMENT")
        )
    )
)

#Define data type
typed_df = (
    staged_df
    .withColumn("PIECES", F.expr("TRY_CAST(PIECES AS INT)"))
    .withColumn(
        "SHIP_WEIGHT", 
        F.expr("TRY_CAST(SHIP_WEIGHT AS DECIMAL(18,3))")
    )
    .withColumn(
        "BILL_WEIGHT",
        F.expr("TRY_CAST(BILL_WEIGHT AS DECIMAL(18,3))")
    )
    .withColumn(
        "FREIGHT_CHARGES",
        F.expr("TRY_CAST(FREIGHT_CHARGES AS DECIMAL(18,2))")
    )
    .withColumn(
        "ACCESSORIAL_CHARGE",
        F.expr("TRY_CAST(ACCESSORIAL_CHARGE AS DECIMAL(18,2))")
    )
    .withColumn(
        "DISCOUNT_AMOUNT",
        F.expr("TRY_CAST(DISCOUNT_AMOUNT AS DECIMAL(18,2))")
    )
    .withColumn(
        "NET_CHARGES",
        F.expr("TRY_CAST(NET_CHARGES AS DECIMAL(18,2))")
    )
    .withColumn(
        "FUEL_CHARGES",
        F.expr("TRY_CAST(FUEL_CHARGES AS DECIMAL(18,2))")
    )
    .withColumn(
        "TOTAL_PAID",
        F.expr("TRY_CAST(TOTAL_PAID AS DECIMAL(18,2))")
    )
    .withColumn(
        "AUDIT_ADJUSTMENT",
        F.expr("TRY_CAST(AUDIT_ADJUSTMENT AS DECIMAL(18,2))")
    )
    .withColumn(
        "DUTY_AND_TAX_CHARGES",
        F.expr("TRY_CAST(DUTY_AND_TAX_CHARGES AS DECIMAL(18,2))")
    )
    .withColumn("SHIP_DATE", F.col("_parsed_ship_date"))
    .withColumn("DELIVERY_DATE", F.col("_parsed_delivery_date"))
    .withColumn("PROCESS_DATE", F.col("_parsed_process_date"))
    .withColumn("INVOICE_DAY_YEAR", F.col("_parsed_invoice_date"))
    .drop(
        "_parsed_ship_date",
        "_parsed_delivery_date",
        "_parsed_process_date",
        "_parsed_invoice_date"
    )
)
# Quarantine disqualified records
quarantine_df = (
    typed_df
    .filter(F.col("_dq_reason")!="")
    .withColumn("_quarantined_at", F.current_timestamp())
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(quarantine_table)
)

#7. deduplicate qualified records
# Keep the latest PACKAGE_GUID when it shows up in multiple data windows
dedup_window= (
    Window
    .partitionBy("PACKAGE_GUID")
    .orderBy(
        F.col("_source_window_end").desc_nulls_last(),
        F.col("_source_file_modified_at").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last()
    )
)

parcel_clean_df = (
    typed_df
    .filter(F.col("_dq_reason") == "")
    .withColumn("_record_rank", F.row_number().over((dedup_window)))
    .filter(F.col("_record_rank") == 1)
    .drop("_record_rank", "_dq_reason")
    .withColumn("_silver_processed_at", F.current_timestamp())
)

(
    parcel_clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(
    f"Silver clean rows:{spark.table(target_table).count():,}"
)

print(
    f"Quarantine rows: {spark.table(quarantine_table).count():,}"
)


# COMMAND ----------

silver_validation = spark.sql(
    f"""
    SELECT
        COUNT(*) AS clean_rows,
        COUNT(DISTINCT PACKAGE_GUID) AS unique_packages,
        COUNT(*) - COUNT(DISTINCT PACKAGE_GUID) AS duplicate_package_rows,
        SUM(
            CASE WHEN PACKAGE_GUID IS NULL THEN 1 ELSE 0 END
        ) AS missing_package_guid_rows,
        SUM(
            CASE WHEN SHIP_DATE IS NULL THEN 1 ELSE 0 END
        ) AS missing_ship_date_rows,
        SUM(
            CASE WHEN DELIVERY_DATE < SHIP_DATE THEN 1 ELSE 0 END
        ) AS delivery_before_shipment_rows,
        COUNT(DISTINCT _source_file_name) AS contributing_source_files,
        MIN(SHIP_DATE) AS earliest_ship_date,
        MAX(SHIP_DATE) AS latest_ship_date

    FROM {target_table}
    """
)

display(silver_validation)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def clean_string_columns(df):
    """strip string"""
    result = df 

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            result = result.withColumn(
                field.name, 
                F.trim(F.col(field.name))
                )
    return result


def apply_lookup_types(table_name, df):
    """Set proper data types for the table"""

    if table_name == "holidays":
        return df.withColumn(
            "HOLIDAY_DATE",
            F.expr("TRY_CAST(HOLIDAY_DATE AS DATE)")
        )

    if table_name == "transit_times":
        return(
            df
            .withColumn(
                "GROUND_TRANSIT_DAYS",
                F.expr("TRY_CAST(GROUND_TRANSIT_DAYS AS INT)")
            )
            .withColumn(
                "SUREPOST_TRANSIT_DAYS",
                F.expr("TRY_CAST(SUREPOST_TRANSIT_DAYS AS INT)")
            )
        )

    if table_name == "vf_calendar":
        return(
            df
            .withColumn(
                "CALENDAR_DATE",
                F.expr("TRY_CAST(CALENDAR_DATE AS DATE)")
            )
            .withColumn(
                "MONTH_NUMBER",
                F.expr("TRY_CAST(MONTH_NUMBER AS INT)")
            )
            .withColumn(
                "YEAR",
                F.expr("TRY_CAST(YEAR AS INT)")
            )
            .withColumn(
                "FISCAL_WEEK",
                F.expr("TRY_CAST(FISCAL_WEEK AS INT)")
            )
        )

    return df


def publish_silver_lookup(table_name, key_columns):
    source = f"{catalog}.bronze.{table_name}"
    target = f"{catalog}.silver.{table_name}"

    lookup_df = clean_string_columns(spark.table(source))
    lookup_df = apply_lookup_types(table_name, lookup_df)

    # Check invalid key
    invalid_key_condition = None

    for key in key_columns:
        current_condition = (
            F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
        )

        invalid_key_condition = (
            current_condition if invalid_key_condition is None else invalid_key_condition | current_condition
        )

    invalid_key_rows = (
        lookup_df
        .filter(invalid_key_condition)
        .count()
    )

    # Check duplicated keys
    duplicate_key_groups = (
        lookup_df
        .groupBy(*key_columns)
        .agg(F.count("*").alias("_key_count"))
        .filter(F.col("_key_count") > 1)
        .count()
    )

    # Abnormal key could cause uncessary expansive join, hence choose STOP policy
    if invalid_key_rows > 0 or duplicate_key_groups > 0:
        raise ValueError(
            f"{table_name} failed validation: "
            f"invalid_key_rows={invalid_key_rows},"
            f"duplicate_key_groups={duplicate_key_groups}"
            )
    
    silver_lookup_df = lookup_df.withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

    (
        silver_lookup_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    print(
        f"Published {target}: "
        f"{spark.table(target).count():,} rows"
    )


lookup_keys = {
    "account_filters": ["ACCOUNT_NUMBER"],
    "account_mapping": ["ACCOUNT_NUMBER"],
    "dc_lookup": ["DC_ZIP_KEY"],
    "holidays": ["HOLIDAY_DATE"],
    "service_levels": ["SERVICE_DESCRIPTION"],
    "transit_times": ["CARRIER_CODE", "ORIGIN_ZIP_KEY", "DESTINATION_ZIP_KEY"],
    "vf_calendar": ["CALENDAR_DATE"],
}

for lookup_name, keys in lookup_keys.items():
    publish_silver_lookup(lookup_name, keys)

# COMMAND ----------

join_coverage = spark.sql(
    f"""
    WITH 
    eligible_parcels AS (
        SELECT p.* FROM {catalog}.silver.parcel_clean AS p

        LEFT SEMI JOIN {catalog}.silver.account_filters AS f ON p.ACCOUNT_NUMBER = f.ACCOUNT_NUMBER
    ),

    joined_parcels AS (
        SELECT
            p.PACKAGE_GUID,

            a.ACCOUNT_NUMBER AS account_match,
            s.SERVICE_DESCRIPTION AS service_match, 
            d.DC_ZIP_KEY AS dc_match, 
            t.ROUTE_KEY AS transit_match, 
            c.CALENDAR_DATE AS calendar_match

        FROM eligible_parcels AS p

        LEFT JOIN {catalog}.silver.account_mapping AS a ON p.ACCOUNT_NUMBER = a.ACCOUNT_NUMBER
        LEFT JOIN {catalog}.silver.service_levels AS s ON p.SERVICE_DESCRIPTION = s.SERVICE_DESCRIPTION
        LEFT JOIN {catalog}.silver.dc_lookup AS d ON p.ORIGIN_ZIP = d.DC_ZIP_KEY
        LEFT JOIN {catalog}.silver.transit_times AS t 
            ON p.CARRIER_CODE = t.CARRIER_CODE
            AND p.ORIGIN_ZIP = t.ORIGIN_ZIP_KEY
            AND p.DESTINATION_ZIP = t.DESTINATION_ZIP_KEY
        LEFT JOIN {catalog}.silver.vf_calendar AS c ON p.SHIP_DATE = c.CALENDAR_DATE
    )

    SELECT
        (SELECT COUNT(*) FROM {catalog}.silver.parcel_clean) AS clean_rows,
        COUNT(*) AS eligible_and_joined_rows,
        COUNT(DISTINCT PACKAGE_GUID) AS unique_joined_packages,
        ROUND(
            100.0 * COUNT(*) / (SELECT COUNT(*) FROM {catalog}.silver.parcel_clean),
            3
        ) AS account_filter_eligibility_pct, 
        ROUND(
            100.0 * SUM(CASE WHEN account_match IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
            3
        ) AS account_mapping_pct,
         ROUND(
            100.0 * SUM(CASE WHEN service_match IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
            3
        ) AS service_match_pct,
        ROUND(
            100.0 * SUM(CASE WHEN dc_match IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
            3
        ) AS dc_match_pct,
        ROUND(
            100.0 * SUM(CASE WHEN transit_match IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
            3
        ) AS transit_match_pct,
        ROUND(
            100.0 * SUM(CASE WHEN calendar_match IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
            3
        ) AS calendar_match_pct

    FROM Joined_parcels 

    """
)

display(join_coverage)

# COMMAND ----------

# DBTITLE 1,Cell 7
enriched_table = f"{catalog}.silver.parcel_enriched"

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {enriched_table} USING DELTA AS
    WITH eligible_parcels AS (
        SELECT p.* FROM {catalog}.silver.parcel_clean AS p
        LEFT SEMI JOIN {catalog}.silver.account_filters AS f ON p.ACCOUNT_NUMBER = f.ACCOUNT_NUMBER
    )
    SELECT
        p.*,

        -- Account enrichment
        a.BRAND AS MAPPED_BRAND,
        a.CHANNEL AS MAPPED_CHANNEL,
        a.CARRIER AS MAPPED_CARRIER,
        a.DC_ID,
        a.NOTE_TOKEN,

        -- Service enrichment
        s.SERVICE_LEVEL,

        -- Distribution center enrichment
        d.DC_ZIP3,
        d.DC_CITY_KEY,
        d.DC_STATE,

        -- Transit enrichment
        t.ROUTE_KEY,
        t.GROUND_TRANSIT_DAYS,
        t.SUREPOST_TRANSIT_DAYS,
        t.ZONE AS TRANSIT_ZONE, 

        -- Fiscal calendar enrichment 
        c.MONTH_NAME AS CALENDAR_MONTH_NAME,
        c.MONTH_NUMBER AS CALENDAR_MONTH_NUMBER,
        c.MONTH_SHORT_NAME AS CALENDAR_MONTH_SHORT_NAME,
        c.FISCAL_QUARTER,
        c.FISCAL_WEEK,
        c.YEAR AS CALENDAR_YEAR,

        -- Individual lookup flags
        a.ACCOUNT_NUMBER IS NOT NULL AS ACCOUNT_MAPPING_MATCHED,
        s.SERVICE_DESCRIPTION IS NOT NULL AS SERVICE_LOOKUP_MATCHED,
        d.DC_ZIP_KEY IS NOT NULL AS DC_LOOKUP_MATCHED,
        t.ROUTE_KEY IS NOT NULL AS TRANSIT_LOOKUP_MATCHED,
        c.CALENDAR_DATE IS NOT NULL AS CALENDAR_LOOKUP_MATCHED,

        -- Overall lookup status
        CASE
            WHEN a.ACCOUNT_NUMBER IS NOT NULL
            AND s.SERVICE_DESCRIPTION IS NOT NULL
            AND d.DC_ZIP_KEY IS NOT NULL
            AND t.ROUTE_KEY IS NOT NULL
            AND c.CALENDAR_DATE IS NOT NULL
            THEN 'COMPLETE'
            ELSE 'PARTIAL'
        END AS LOOKUP_QUALITY_STATUS,

        CONCAT_WS(
            ', ',
            CASE WHEN a.ACCOUNT_NUMBER IS NULL THEN 'ACCOUNT_MAPPING' END,
            CASE WHEN s.SERVICE_DESCRIPTION IS NULL THEN 'SERVICE_LEVEL' END,
            CASE WHEN d.DC_ZIP_KEY IS NULL THEN 'DC_LOOKUP' END, 
            CASE WHEN t.ROUTE_KEY IS NULL THEN 'TRANSIT_TIME' END,
            CASE WHEN c.CALENDAR_DATE IS NULL THEN 'FISCAL_CALENDAR' END
        ) AS MISSING_LOOKUPS, 

        CURRENT_TIMESTAMP() AS _enriched_at
    
    FROM eligible_parcels AS p

    LEFT JOIN {catalog}.silver.account_mapping AS a ON p.ACCOUNT_NUMBER = a.ACCOUNT_NUMBER
    LEFT JOIN {catalog}.silver.service_levels AS s ON p.SERVICE_DESCRIPTION = s.SERVICE_DESCRIPTION
    LEFT JOIN {catalog}.silver.dc_lookup AS d ON p.ORIGIN_ZIP = d.DC_ZIP_KEY
    LEFT JOIN {catalog}.silver.transit_times AS t
        ON p.CARRIER_CODE = t.CARRIER_CODE
        AND p.ORIGIN_ZIP = t.ORIGIN_ZIP_KEY
        AND p.DESTINATION_ZIP = t.DESTINATION_ZIP_KEY
    LEFT JOIN {catalog}.silver.vf_calendar AS c ON p.SHIP_DATE = c.CALENDAR_DATE
    """
)

print(f"Created {enriched_table}")

# COMMAND ----------

enriched_validation = spark.sql(
    f"""
    SELECT
        COUNT(*) AS enriched_rows,
        COUNT(DISTINCT PACKAGE_GUID) AS unique_packages,
        COUNT(*) - COUNT(DISTINCT PACKAGE_GUID) AS duplicate_package_rows,
        SUM(
            CASE WHEN LOOKUP_QUALITY_STATUS = 'COMPLETE' THEN 1 ELSE 0 END
        ) AS complete_lookup_rows,
        SUM(
            CASE WHEN LOOKUP_QUALITY_STATUS = 'PARTIAL' THEN 1 ELSE 0 END
        ) AS partial_lookup_rows,
        SUM(
            CASE WHEN NOT ACCOUNT_MAPPING_MATCHED THEN 1 ELSE 0 END
        ) AS missing_account_mapping_rows,
        SUM(
            CASE WHEN NOT SERVICE_LOOKUP_MATCHED THEN 1 ELSE 0 END
        ) AS missing_service_rows,
        SUM(
            CASE WHEN NOT DC_LOOKUP_MATCHED THEN 1 ELSE 0 END
        ) AS missing_dc_rows,
        SUM(
            CASE WHEN NOT TRANSIT_LOOKUP_MATCHED THEN 1 ELSE 0 END
        ) AS missing_transit_rows,
        SUM(
            CASE WHEN NOT CALENDAR_LOOKUP_MATCHED THEN 1 ELSE 0 END
        ) AS missing_calendar_rows

    From {enriched_table}
    """
)

display(enriched_validation)

# COMMAND ----------

from pyspark.sql import functions as F

performance_table = f"{catalog}.silver.parcel_performance"

sla_base_df = spark.sql(
    f"""
    WITH 
    holiday_set AS(
        SELECT COLLECT_SET(HOLIDAY_DATE) AS holiday_dates 
        FROM {catalog}.silver.holidays
    ),

    promise_base AS (
        SELECT 
            e.*,
            CASE
                WHEN UPPER(CARRIER_CODE) = 'PITNEY_BOWES' THEN 3
                WHEN SERVICE_LEVEL = 'Overnight' THEN 1
                WHEN SERVICE_LEVEL = '2-Day' THEN 2
                WHEN SERVICE_LEVEL = '3-Day'  THEN 3
                ELSE GROUND_TRANSIT_DAYS
            END AS BASE_PROMISED_TRANSIT_DAYS
        
        FROM {catalog}.silver.parcel_enriched AS e
    ),

    promised AS(
        SELECT
            *,
            CAST(
                CASE
                    WHEN UPPER(SERVICE_DESCRIPTION) LIKE '%SUREPOST%' THEN BASE_PROMISED_TRANSIT_DAYS + 2
                    WHEN UPPER(SERVICE_DESCRIPTION) LIKE '%GROUND%' AND UPPER(SERVICE_DESCRIPTION) LIKE '%ECONOMY%' THEN BASE_PROMISED_TRANSIT_DAYS + 2
                    ELSE BASE_PROMISED_TRANSIT_DAYS
                END AS INT
            )
        AS PROMISED_TRANSIT_DAYS
        FROM promise_base
    )

    SELECT
        p.*,
        CASE
            WHEN DELIVERY_DATE IS NULL THEN NULL
            WHEN DELIVERY_DATE <= SHIP_DATE THEN 0
            ELSE SIZE(
                FILTER(
                    SEQUENCE(
                        SHIP_DATE,
                        DATE_SUB(DELIVERY_DATE, 1)
                    ),
                    business_date -> WEEKDAY(business_date) BETWEEN 0 AND 4 and NOT ARRAY_CONTAINS(h.holiday_dates, business_date)
                )
            )
        END AS ACTUAL_TRANSIT_DAYS
    FROM promised AS p
    CROSS JOIN holiday_set AS h
    """
)

performance_df = (
    sla_base_df
    .drop("BASE_PROMISED_TRANSIT_DAYS")
    .withColumn(
        "ON_TIME_FLAG",
        F.when(
            F.col("DELIVERY_DATE").isNull() | F.col("PROMISED_TRANSIT_DAYS").isNull(),
            F.lit(None).cast("int")
        )
        .when(
            F.col("ACTUAL_TRANSIT_DAYS") <= F.col("PROMISED_TRANSIT_DAYS"),
            F.lit(1)
        )
        .otherwise(F.lit(0))
    )

    .withColumn(
        "DELIVERY_STATUS",
        F.when(
            F.col("DELIVERY_DATE").isNull(),
            F.lit("NO_DELIVERY_DATE")
        )
        .when(
            F.col("PROMISED_TRANSIT_DAYS").isNull(),
            F.lit("MISSING_PROMISE")
        )
        .when(
            F.col("ACTUAL_TRANSIT_DAYS") <= F.col("PROMISED_TRANSIT_DAYS"),
            F.lit("ON_TIME")
        )
        .otherwise(F.lit("LATE"))
    )

    .withColumn(
        "DAYS_LATE",
        F.when(
            F.col("ACTUAL_TRANSIT_DAYS").isNull() | F.col("PROMISED_TRANSIT_DAYS").isNull(),
            F.lit(None).cast("int")
        )
        .otherwise(
            F.greatest(
                F.col("ACTUAL_TRANSIT_DAYS") - F.col("PROMISED_TRANSIT_DAYS"),
                F.lit(0)
            )
        )
    )
    
    .withColumn(
        "_sla_calculated_at",
        F.current_timestamp()
    )
)

required_columns = [
    "PROMISED_TRANSIT_DAYS",
    "ACTUAL_TRANSIT_DAYS",
    "ON_TIME_FLAG",
    "DELIVERY_STATUS",
    "DAYS_LATE",
]

missing_columns = [column for column in required_columns if column not in performance_df.columns]

if missing_columns:
    raise ValueError(f"Missing calculated columns: {missing_columns}")

(
    performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(performance_table)
)

print(f"Created {performance_table}")
print(f"Rows: {spark.table(performance_table).count():,}")
print("SLA columns:", required_columns)

# COMMAND ----------

sla_validation = spark.sql(
    f"""
    SELECt 
        COUNT(*) AS performance_rows,
        COUNT(DISTINCT PACKAGE_GUID) AS unique_packages, 
        COUNT(*) - COUNT(DISTINCT PACKAGE_GUID) AS duplicate_package_rows,
        SUM(
            CASE WHEN DELIVERY_STATUS = 'ON_TIME' THEN 1 ELSE 0 END
        ) AS on_time_rows,
        SUM(
            CASE WHEN DELIVERY_STATUS = 'LATE' THEN 1 ELSE 0 END
        ) AS late_rows, 
        SUM(
            CASE WHEN DELIVERY_STATUS = 'NO_DELIVERY_DATE' THEN 1 ELSE 0 END
        ) AS no_delivery_date_rows,
        SUM(
            CASE WHEN DELIVERY_STATUS = 'MISSING_PROMISE' THEN 1 ELSE 0 END
        ) AS missing_promise_rows,
        ROUND(
            100.0 * SUM(ON_TIME_FLAG)  / COUNT(ON_TIME_FLAG),
            2
        ) AS measured_on_time_pct,
        ROUND(
            AVG(ACTUAL_TRANSIT_DAYS),
            2
        ) AS average_actual_transit_days,
        ROUND(
            AVG(PROMISED_TRANSIT_DAYS),
            2
        ) AS average_promised_transit_days,
        MAX(DAYS_LATE) AS maximum_days_late
    FROM {performance_table}
    """
)

display(sla_validation)

# COMMAND ----------

