# Databricks notebook source
from pyspark.sql import functions as F

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

source_path = f"/Volumes/{catalog}/bronze/landing/parcel_windows"
state_path = f"/Volumes/{catalog}/ops/pipeline_state"

schema_path = f"{state_path}/schemas/parcel_raw"
checkpoint_path = f"{state_path}/checkpoints/parcel_raw"
target_table = f"{catalog}.bronze.parcel_raw"

print(f"Catalog:      {catalog}")
print(f"Source:       {source_path}")
print(f"Checkpoint:   {checkpoint_path}")
print(f"Target:       {target_table}")

# COMMAND ----------

bronze_stream = (
    spark .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header","true")
    .option("sep", "\t")
    .option("encoding", "UTF-8")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(source_path)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias(
            "_source_file_modified_at"
        ),
        F.current_timestamp().alias("_ingested_at")
    )
)

query = (
    bronze_stream.writeStream
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()

print("Bronze ingestion completed.")

# COMMAND ----------

bronze_metrics = spark.sql(
    f"""
    SELECT
      COUNT(*) AS landed_rows,
      COUNT(DISTINCT PACKAGE_GUID) AS unique_packages,
      COUNT(DISTINCT _source_file_name) AS source_files,
      SUM(
          CASE WHEN _rescued_data  IS NOT NULL THEN 1 ELSE 0 END
      ) AS rescued_rows,
      ROUND(
          COUNT(*)/ COUNT(DISTINCT PACKAGE_GUID),
          3
      ) AS landing_amplification
      FROM {target_table}
    """
)

display(bronze_metrics)

# COMMAND ----------

from pyspark.sql import functions as F

lookup_source_path = f"/Volumes/{catalog}/bronze/landing/lookups"

lookup_files = {
    "account_filters": "account_filters.tsv",
    "account_mapping": "account_mapping.tsv",
    "dc_lookup": "dc_lookup.tsv",
    "holidays": "holidays.tsv",
    "service_levels": "service_levels.tsv",
    "transit_times": "transit_times.tsv",
    "vf_calendar": "vf_calendar.tsv",
}

for table_name, file_name in lookup_files.items():
    file_path = f"{lookup_source_path}/{file_name}"
    target_table = f"{catalog}.bronze.{table_name}"

    lookup_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .load(file_path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias(
                "_source_file_modified_at"
            ),
            F.current_timestamp().alias("_ingested_at"),
        )
    )

    (
        lookup_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    row_count = spark.table(target_table).count()
    print(f"Loaded {target_table}: {row_count:,} rows")


# COMMAND ----------

bronze_tables = [
    "parcel_raw",
    "account_filters",
    "account_mapping",
    "dc_lookup",
    "holidays",
    "service_levels",
    "transit_times",
    "vf_calendar",
]

validation_results = []

for table_name in bronze_tables:
    df = spark.table(f"{catalog}.bronze.{table_name}")

    validation_results.append(
        (
            table_name,
            df.count(),
            len(df.columns),
        )
    )

bronze_inventory = spark.createDataFrame(
    validation_results,
    ["table_name", "row_count", "column_count"]
)

display(bronze_inventory.orderBy("table_name"))

# COMMAND ----------

