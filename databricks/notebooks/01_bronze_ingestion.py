# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC **Pipeline:** Raw Zone → Bronze Delta Tables
# MAGIC
# MAGIC Appends raw source data with metadata columns. No business transformations.
# MAGIC Schema enforcement applied to catch upstream drift early.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit,
    col, to_date, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# All secrets pulled from Azure Key Vault — never hardcode credentials
storage_account  = dbutils.secrets.get(scope="kv-datapipeline", key="storage-account-name")
storage_key      = dbutils.secrets.get(scope="kv-datapipeline", key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Path configuration (parameterized for dev/prod)
env         = dbutils.widgets.get("env") if dbutils.widgets.getArgument("env", "") else "dev"
base_path   = f"abfss://datalake@{storage_account}.dfs.core.windows.net"
raw_path    = f"{base_path}/raw/orders"
bronze_path = f"{base_path}/bronze/orders"

print(f"Environment : {env}")
print(f"Raw path    : {raw_path}")
print(f"Bronze path : {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition
# MAGIC
# MAGIC Enforcing schema at bronze prevents silent type corruption
# MAGIC from upstream source changes.

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id",         StringType(),  nullable=False),
    StructField("customer_id",      StringType(),  nullable=True),
    StructField("product_id",       StringType(),  nullable=True),
    StructField("product_category", StringType(),  nullable=True),
    StructField("order_date",       StringType(),  nullable=True),  # cast in Silver
    StructField("order_amount",     DoubleType(),  nullable=True),
    StructField("quantity",         IntegerType(), nullable=True),
    StructField("region",           StringType(),  nullable=True),
    StructField("status",           StringType(),  nullable=True),
    StructField("last_modified",    StringType(),  nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Load — Watermark Pattern
# MAGIC
# MAGIC Only reads files modified after the last successful run.
# MAGIC Watermark stored in a Delta control table.

# COMMAND ----------

control_table_path = f"{base_path}/control/watermark"

def get_last_watermark(table_path: str, source: str) -> str:
    """Read the last processed timestamp from the control table."""
    try:
        df_control = spark.read.format("delta").load(table_path)
        row = df_control.filter(col("source_name") == source).orderBy(
            col("watermark_ts").desc()
        ).first()
        return row["watermark_ts"] if row else "1900-01-01T00:00:00"
    except Exception:
        logger.warning("Control table not found — running full load.")
        return "1900-01-01T00:00:00"


def update_watermark(table_path: str, source: str, new_ts: str):
    """Write the new high-watermark after a successful run."""
    from pyspark.sql import Row
    row = Row(source_name=source, watermark_ts=new_ts,
              updated_at=str(current_timestamp()))
    df_wm = spark.createDataFrame([row])
    df_wm.write.format("delta").mode("append").save(table_path)


last_wm = get_last_watermark(control_table_path, "orders")
print(f"Last watermark: {last_wm}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Data

# COMMAND ----------

df_raw = (
    spark.read
    .schema(orders_schema)
    .option("mergeSchema", "false")        # Strict — reject schema drift
    .option("badRecordsPath", f"{base_path}/quarantine/orders")  # Bad rows isolated
    .parquet(raw_path)
    .filter(col("last_modified") > last_wm)
)

raw_count = df_raw.count()
print(f"New records since {last_wm}: {raw_count:,}")

if raw_count == 0:
    print("No new records — pipeline will exit cleanly.")
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Metadata Columns & Write to Bronze

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file",         input_file_name())
    .withColumn("_pipeline_run_id",     lit(dbutils.notebook.entry_point
                                           .getDbutils().notebook()
                                           .getContext().currentRunId()
                                           .getOrElse(None)))
    .withColumn("_env",                 lit(env))
    # Partition columns for efficient downstream reads
    .withColumn("year",  year(to_date(col("order_date"),  "yyyy-MM-dd")))
    .withColumn("month", month(to_date(col("order_date"), "yyyy-MM-dd")))
    .withColumn("day",   dayofmonth(to_date(col("order_date"), "yyyy-MM-dd")))
)

# COMMAND ----------

# Write as Delta — append only (Bronze never modifies source data)
(
    df_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("year", "month", "day")
    .option("mergeSchema", "false")
    .save(bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Gate
# MAGIC
# MAGIC Fail the pipeline early if quality thresholds are breached.

# COMMAND ----------

bronze_written = spark.read.format("delta").load(bronze_path)
latest_count   = bronze_written.filter(col("_ingestion_timestamp") >= current_timestamp()).count()

# Quality checks
null_order_ids = df_bronze.filter(col("order_id").isNull()).count()
assert null_order_ids == 0, f"QUALITY FAIL: {null_order_ids} null order_ids found in Bronze"

negative_amounts = df_bronze.filter(col("order_amount") < 0).count()
if negative_amounts > 0:
    logger.warning(f"WARNING: {negative_amounts} negative order amounts — will be filtered in Silver")

print(f"✅ Bronze write successful")
print(f"   Records written   : {raw_count:,}")
print(f"   Null order_ids    : {null_order_ids}")
print(f"   Negative amounts  : {negative_amounts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Watermark

# COMMAND ----------

new_watermark = df_raw.agg({"last_modified": "max"}).collect()[0][0]
update_watermark(control_table_path, "orders", new_watermark)
print(f"✅ Watermark updated to: {new_watermark}")

# COMMAND ----------

# Return summary for ADF pipeline monitoring
dbutils.notebook.exit({
    "status":            "SUCCESS",
    "records_ingested":  raw_count,
    "new_watermark":     new_watermark,
    "bronze_path":       bronze_path
})
