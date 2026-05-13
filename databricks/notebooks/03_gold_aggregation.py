# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations
# MAGIC **Pipeline:** Silver Delta → Gold Delta Tables
# MAGIC
# MAGIC Produces four business-ready aggregate tables consumed directly by Power BI.
# MAGIC All aggregations are idempotent — safe to re-run without side effects.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max, min as _min,
    round as _round, countDistinct, when, lit,
    current_timestamp, date_trunc, to_date,
    lag, coalesce, expr
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

# COMMAND ----------

storage_account = dbutils.secrets.get(scope="kv-datapipeline", key="storage-account-name")
storage_key     = dbutils.secrets.get(scope="kv-datapipeline", key="storage-account-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

base_path   = f"abfss://datalake@{storage_account}.dfs.core.windows.net"
silver_path = f"{base_path}/silver/orders"
gold_path   = f"{base_path}/gold"

# COMMAND ----------

# Read Silver — completed orders only (exclude cancelled/pending for revenue)
df_silver = spark.read.format("delta").load(silver_path)
df_revenue = df_silver.filter(col("status").isin("COMPLETED", "REFUNDED"))
print(f"Silver records (completed + refunded): {df_revenue.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1 — Daily Revenue by Region & Category
# MAGIC
# MAGIC Primary table for the main Power BI dashboard page.

# COMMAND ----------

df_daily_revenue = (
    df_revenue
    .groupBy("order_date", "region", "product_category")
    .agg(
        _round(_sum(
            when(col("status") == "COMPLETED", col("order_amount"))
            .when(col("status") == "REFUNDED",  -col("order_amount"))
            .otherwise(0)
        ), 2).alias("net_revenue"),

        count(when(col("status") == "COMPLETED", True)).alias("completed_orders"),
        count(when(col("status") == "REFUNDED",  True)).alias("refunded_orders"),

        _round(avg(col("order_amount")), 2).alias("avg_order_value"),
        _sum(col("quantity")).alias("total_units_sold"),
        countDistinct(col("customer_id")).alias("unique_customers")
    )
    .withColumn("refund_rate",
        _round(
            col("refunded_orders") / (col("completed_orders") + col("refunded_orders")) * 100,
            2
        )
    )
    .withColumn("_gold_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2 — Month-over-Month Revenue Trend
# MAGIC
# MAGIC Powers the trend line chart in Power BI.

# COMMAND ----------

df_monthly = (
    df_revenue
    .filter(col("status") == "COMPLETED")
    .withColumn("month_start", date_trunc("month", col("order_date")))
    .groupBy("month_start", "region")
    .agg(
        _round(_sum("order_amount"), 2).alias("monthly_revenue"),
        count("order_id").alias("monthly_orders"),
        countDistinct("customer_id").alias("unique_customers")
    )
)

# Month-over-month growth % using lag window
window_mom = Window.partitionBy("region").orderBy("month_start")

df_mom = (
    df_monthly
    .withColumn("prev_month_revenue",
        lag("monthly_revenue", 1).over(window_mom))
    .withColumn("mom_growth_pct",
        _round(
            (col("monthly_revenue") - col("prev_month_revenue"))
            / col("prev_month_revenue") * 100,
            2
        )
    )
    .withColumn("_gold_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3 — Customer Segments (RFM Summary)
# MAGIC
# MAGIC Recency, Frequency, Monetary value — simplified for Power BI customer analysis.

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date

max_date = df_silver.agg(_max("order_date")).collect()[0][0]

df_rfm = (
    df_silver
    .filter(col("status") == "COMPLETED")
    .groupBy("customer_id")
    .agg(
        datediff(lit(max_date), _max("order_date")).alias("recency_days"),
        count("order_id").alias("frequency"),
        _round(_sum("order_amount"), 2).alias("monetary_value"),
        _round(avg("order_amount"), 2).alias("avg_order_value"),
        _max("order_date").alias("last_order_date"),
        _min("order_date").alias("first_order_date")
    )
    .withColumn("customer_segment",
        when(col("recency_days") <= 30,  "Champion")
        .when(col("recency_days") <= 90,  "Loyal")
        .when(col("recency_days") <= 180, "At Risk")
        .otherwise("Lost")
    )
    .withColumn("_gold_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4 — Product Performance
# MAGIC
# MAGIC Top/bottom products by revenue and volume.

# COMMAND ----------

df_product = (
    df_revenue
    .filter(col("status") == "COMPLETED")
    .groupBy("product_id", "product_category")
    .agg(
        _round(_sum("order_amount"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders"),
        _sum("quantity").alias("total_units_sold"),
        _round(avg("order_amount"), 2).alias("avg_selling_price"),
        countDistinct("customer_id").alias("unique_buyers")
    )
    .withColumn("revenue_rank",
        col("total_revenue").cast("double"))
    .withColumn("_gold_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write All Gold Tables

# COMMAND ----------

def write_gold_table(df, table_name: str, partition_cols: list = None):
    """Write a Gold table, replacing only today's partition (idempotent)."""
    path = f"{gold_path}/{table_name}"
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.option("overwriteSchema", "true").save(path)
    count = spark.read.format("delta").load(path).count()
    print(f"✅ Gold/{table_name}: {count:,} rows")
    return count

counts = {}
counts["daily_revenue"]      = write_gold_table(df_daily_revenue, "daily_revenue",
                                                  ["order_date"])
counts["monthly_trend"]      = write_gold_table(df_mom, "monthly_trend",
                                                  ["region"])
counts["customer_segments"]  = write_gold_table(df_rfm, "customer_segments")
counts["product_performance"] = write_gold_table(df_product, "product_performance",
                                                   ["product_category"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register as Hive Tables (Synapse / SQL Endpoint access)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gold_db")

for table_name in counts.keys():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_db.{table_name}
        USING DELTA
        LOCATION '{gold_path}/{table_name}'
    """)
    print(f"✅ Registered: gold_db.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER — BUILD COMPLETE")
print("=" * 60)
for table, cnt in counts.items():
    print(f"  {table:<30} {cnt:>10,} rows")
print("=" * 60)

dbutils.notebook.exit({
    "status": "SUCCESS",
    "gold_tables": counts,
    "gold_path": gold_path
})
