# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleansing & Standardization
# MAGIC **Pipeline:** Bronze Delta → Silver Delta
# MAGIC
# MAGIC Applies business rules: deduplication, type casting, null handling,
# MAGIC string normalization. Output is reliable and queryable by analysts.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, upper, trim, when, coalesce,
    lit, current_timestamp, regexp_replace,
    row_number, desc, count, sum as _sum
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

storage_account = dbutils.secrets.get(scope="kv-datapipeline", key="storage-account-name")
storage_key     = dbutils.secrets.get(scope="kv-datapipeline", key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

base_path    = f"abfss://datalake@{storage_account}.dfs.core.windows.net"
bronze_path  = f"{base_path}/bronze/orders"
silver_path  = f"{base_path}/silver/orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze (only unprocessed records)

# COMMAND ----------

# Read only records not yet processed into Silver
# Using _ingestion_timestamp as our Silver watermark
silver_wm_path = f"{base_path}/control/silver_watermark"

def get_silver_watermark():
    try:
        return (spark.read.format("delta").load(silver_wm_path)
                .filter(col("source") == "bronze_orders")
                .orderBy(desc("watermark_ts"))
                .first()["watermark_ts"])
    except Exception:
        return "1900-01-01T00:00:00"

silver_wm = get_silver_watermark()
print(f"Silver watermark: {silver_wm}")

df_bronze = (
    spark.read.format("delta").load(bronze_path)
    .filter(col("_ingestion_timestamp") > silver_wm)
)

bronze_count = df_bronze.count()
print(f"Bronze records to process: {bronze_count:,}")

if bronze_count == 0:
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Deduplication
# MAGIC
# MAGIC Keep the latest record per `order_id` using a window function.
# MAGIC This handles cases where the same order appears multiple times
# MAGIC due to late-arriving data or API retries.

# COMMAND ----------

window_spec = Window.partitionBy("order_id").orderBy(desc("last_modified"))

df_deduped = (
    df_bronze
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

duplicates_removed = bronze_count - df_deduped.count()
print(f"Duplicates removed: {duplicates_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Type Casting & Date Parsing

# COMMAND ----------

df_typed = (
    df_deduped
    # Parse order_date string → proper DateType
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    # Ensure numeric types are correct
    .withColumn("order_amount", col("order_amount").cast("double"))
    .withColumn("quantity",     col("quantity").cast("integer"))
)

# Check for unparseable dates (will be null after to_date)
bad_dates = df_typed.filter(col("order_date").isNull()).count()
if bad_dates > 0:
    logger.warning(f"{bad_dates} records with unparseable dates — routing to quarantine")
    (df_typed.filter(col("order_date").isNull())
     .write.format("delta").mode("append")
     .save(f"{base_path}/quarantine/silver_bad_dates"))

df_typed = df_typed.filter(col("order_date").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — String Standardization

# COMMAND ----------

df_clean = (
    df_typed
    # Normalize region: "north east" / "NORTH EAST" / " North East " → "NORTH_EAST"
    .withColumn("region",
        upper(trim(regexp_replace(col("region"), r"\s+", "_"))))
    # Normalize product_category
    .withColumn("product_category",
        upper(trim(col("product_category"))))
    # Normalize status values
    .withColumn("status",
        upper(trim(col("status"))))
    # Standardize customer_id format
    .withColumn("customer_id",
        upper(trim(col("customer_id"))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Business Rule Validation

# COMMAND ----------

df_valid = (
    df_clean
    # Remove physically impossible values
    .filter(col("order_amount") > 0)
    .filter(col("quantity") > 0)
    # Only process known statuses
    .filter(col("status").isin(
        "COMPLETED", "PENDING", "CANCELLED", "REFUNDED", "PROCESSING"
    ))
    # Null-safe customer_id (use placeholder if missing)
    .withColumn("customer_id",
        coalesce(col("customer_id"), lit("UNKNOWN")))
)

invalid_removed = df_deduped.count() - df_valid.count()
print(f"Invalid records removed: {invalid_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Add Silver Metadata & Write (MERGE / Upsert)
# MAGIC
# MAGIC Using Delta MERGE instead of overwrite to handle
# MAGIC late-arriving corrections to existing orders.

# COMMAND ----------

df_silver_new = (
    df_valid
    .withColumn("_silver_timestamp", current_timestamp())
    .drop("_ingestion_timestamp", "_source_file",
          "_pipeline_run_id", "_env")
    .select(
        "order_id", "customer_id", "product_id",
        "product_category", "order_date", "order_amount",
        "quantity", "region", "status", "last_modified",
        "year", "month", "day", "_silver_timestamp"
    )
)

# COMMAND ----------

# Check if Silver table exists — first run does a full write
try:
    silver_table = DeltaTable.forPath(spark, silver_path)

    # MERGE: update existing orders, insert new ones
    (silver_table.alias("silver")
     .merge(
         df_silver_new.alias("new"),
         "silver.order_id = new.order_id"
     )
     .whenMatchedUpdate(
         condition="silver.last_modified < new.last_modified",
         set={
             "order_amount":     "new.order_amount",
             "status":           "new.status",
             "last_modified":    "new.last_modified",
             "_silver_timestamp":"new._silver_timestamp"
         }
     )
     .whenNotMatchedInsertAll()
     .execute())
    print("✅ Silver MERGE complete")

except Exception:
    # First-time write
    (df_silver_new.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("year", "month")
     .save(silver_path))
    print("✅ Silver initial write complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Optimize Silver Table

# COMMAND ----------

# Run OPTIMIZE + ZORDER monthly (controlled by job schedule)
run_optimize = dbutils.widgets.get("run_optimize") if dbutils.widgets.getArgument("run_optimize", "") else "false"

if run_optimize.lower() == "true":
    spark.sql(f"""
        OPTIMIZE delta.`{silver_path}`
        ZORDER BY (order_date, region, product_category)
    """)
    print("✅ OPTIMIZE + ZORDER complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Summary

# COMMAND ----------

df_silver_check = spark.read.format("delta").load(silver_path)

print("=" * 50)
print("SILVER LAYER — QUALITY SUMMARY")
print("=" * 50)
print(f"Total records in Silver : {df_silver_check.count():,}")
print(f"Bronze records received : {bronze_count:,}")
print(f"Duplicates removed      : {duplicates_removed:,}")
print(f"Invalid records removed : {invalid_removed:,}")
print(f"Bad dates quarantined   : {bad_dates:,}")
print("=" * 50)

# Update silver watermark
from pyspark.sql import Row
new_wm = df_bronze.agg({"_ingestion_timestamp": "max"}).collect()[0][0]
spark.createDataFrame([Row(source="bronze_orders", watermark_ts=str(new_wm))]) \
     .write.format("delta").mode("append").save(silver_wm_path)

dbutils.notebook.exit({
    "status":           "SUCCESS",
    "records_processed": bronze_count,
    "records_written":   df_silver_check.count(),
    "silver_path":       silver_path
})
