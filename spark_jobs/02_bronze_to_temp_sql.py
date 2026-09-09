import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# ARGUMENTS
# ============================================================

if len(sys.argv) not in {3, 4}:
    raise ValueError(
        "Usage: 02_bronze_to_temp_sql.py START_DATE END_DATE [MODE]"
    )

START_DATE = sys.argv[1]
END_DATE = sys.argv[2]
MODE = sys.argv[3] if len(sys.argv) == 4 else "backfill"

start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

if start_dt > end_dt:
    raise ValueError("START_DATE must be <= END_DATE")


# ============================================================
# POSTGRES ENVIRONMENT
# ============================================================

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "warehouse")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

if not PGUSER:
    raise RuntimeError("PGUSER environment variable is missing")

if not PGPASSWORD:
    raise RuntimeError("PGPASSWORD environment variable is missing")

JDBC_URL = (
    f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
)

JDBC_TABLE = "temp.retail_batch"


# ============================================================
# PATH
# ============================================================

BRONZE_BASE = "/opt/pipeline/data/bronze/retail"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName(
        f"bronze-to-temp-{START_DATE}-{END_DATE}"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "16"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 80)
print("BRONZE -> POSTGRESQL TEMP")
print(f"START_DATE : {START_DATE}")
print(f"END_DATE   : {END_DATE}")
print(f"JDBC HOST  : {PGHOST}")
print(f"DATABASE   : {PGDATABASE}")
print("=" * 80)


# ============================================================
# READ SELECTED BRONZE PARTITIONS
# ============================================================

try:
    bronze_df = spark.read.option("basePath", BRONZE_BASE).parquet(BRONZE_BASE)
except Exception:
    run_token = os.getenv("CDC_RUN_TOKEN")
    if not run_token:
        raise
    bronze_df = (
        spark.read.parquet(
            f"/opt/pipeline/data/cdc/active/{run_token}/candidate_snapshot"
        )
        .withColumn("_bronze_loaded_at", F.current_timestamp())
        .withColumn("_process_date", F.col("_batch_date").cast("date"))
        .withColumn("_pipeline_name", F.lit("online_retail"))
        .withColumn("invoice_date", F.col("_batch_date").cast("date"))
    )

bronze_df = bronze_df.filter(
    (F.col("invoice_date") >= F.lit(START_DATE).cast("date"))
    & (F.col("invoice_date") <= F.lit(END_DATE).cast("date"))
)


# ============================================================
# BUILD SQL STAGING DATAFRAME
# ============================================================

stage_df = (
    bronze_df.select(

        F.col("InvoiceNo")
        .cast("string")
        .alias("invoice_no_raw"),

        F.col("StockCode")
        .cast("string")
        .alias("stock_code_raw"),

        F.col("Description")
        .cast("string")
        .alias("description_raw"),

        F.col("Quantity")
        .cast("string")
        .alias("quantity_raw"),

        F.col("InvoiceDate")
        .cast("string")
        .alias("invoice_date_raw"),

        F.col("UnitPrice")
        .cast("string")
        .alias("unit_price_raw"),

        F.col("CustomerID")
        .cast("string")
        .alias("customer_id_raw"),

        F.col("Country")
        .cast("string")
        .alias("country_raw"),

        F.col("_invoice_timestamp")
        .cast("timestamp")
        .alias("invoice_timestamp"),

        # Đây là ngày thật của từng row.
        # KHÔNG dùng START_DATE cho tất cả rows.
        F.col("invoice_date")
        .cast("date")
        .alias("batch_date"),

        F.col("_row_hash")
        .cast("string")
        .alias("row_hash"),

        F.col("_source_file")
        .cast("string")
        .alias("source_file"),

        F.col("_converted_at")
        .cast("timestamp")
        .alias("converted_at"),

        F.col("_bronze_loaded_at")
        .cast("timestamp")
        .alias("bronze_loaded_at"),

        F.col("_process_date")
        .cast("date")
        .alias("process_date"),

        F.col("_pipeline_name")
        .cast("string")
        .alias("pipeline_name"),

        F.lit(START_DATE)
        .cast("date")
        .alias("run_start_date"),

        F.lit(END_DATE)
        .cast("date")
        .alias("run_end_date"),

        F.current_timestamp()
        .alias("temp_loaded_at")
    )
)


stage_df.cache()

source_count = stage_df.count()

print(f"BRONZE RANGE COUNT = {source_count}")


# ============================================================
# LOAD POSTGRES JDBC DRIVER
# ============================================================

jvm = spark.sparkContext._gateway.jvm

jvm.java.lang.Class.forName(
    "org.postgresql.Driver"
)


# ============================================================
# TRUNCATE CURRENT STAGING BATCH
# ============================================================

connection = None
statement = None

try:
    connection = jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        PGUSER,
        PGPASSWORD
    )

    statement = connection.createStatement()

    statement.execute(
        "TRUNCATE TABLE temp.retail_batch"
    )

    print("TEMP TABLE TRUNCATED")

finally:
    if statement is not None:
        statement.close()

    if connection is not None:
        connection.close()


# ============================================================
# INSERT SELECTED RANGE
# ============================================================

if source_count > 0:
    (
        stage_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", JDBC_TABLE)
        .option("user", PGUSER)
        .option("password", PGPASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "5000")
        .option("numPartitions", "4")
        .mode("append")
        .save()
    )

print("SPARK JDBC WRITE COMPLETE")


# ============================================================
# READ TEMP TABLE BACK
# ============================================================

temp_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", JDBC_TABLE)
    .option("user", PGUSER)
    .option("password", PGPASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

temp_df.cache()

target_count = temp_df.count()


# ============================================================
# TOTAL COUNT RECONCILIATION
# ============================================================

print()
print("=" * 80)
print("TOTAL COUNT RECONCILIATION")
print(f"BRONZE = {source_count}")
print(f"TEMP   = {target_count}")
print(f"DIFF   = {source_count - target_count}")

if source_count != target_count:
    raise RuntimeError(
        "BRONZE -> TEMP total count reconciliation FAILED"
    )

print("TOTAL COUNT STATUS = PASS")


# ============================================================
# PER-DATE RECONCILIATION
# ============================================================

bronze_daily = (
    stage_df
    .groupBy("batch_date")
    .count()
    .withColumnRenamed(
        "count",
        "bronze_count"
    )
)

temp_daily = (
    temp_df
    .groupBy("batch_date")
    .count()
    .withColumnRenamed(
        "count",
        "temp_count"
    )
)

daily_check = (
    bronze_daily
    .join(
        temp_daily,
        ["batch_date"],
        "full"
    )
    .fillna(0)
    .withColumn(
        "difference",
        F.col("bronze_count") - F.col("temp_count")
    )
)

print()
print("PER-DATE RECONCILIATION")

daily_check.orderBy(
    "batch_date"
).show(
    1000,
    truncate=False
)

failed_days = (
    daily_check
    .filter(
        F.col("difference") != 0
    )
    .count()
)

if failed_days != 0:
    raise RuntimeError(
        f"Per-date reconciliation FAILED: {failed_days} day(s)"
    )

print("PER-DATE STATUS = PASS")


# ============================================================
# TWO-WAY HASH RECONCILIATION
# ============================================================

bronze_hashes = (
    stage_df
    .select("row_hash")
)

temp_hashes = (
    temp_df
    .select("row_hash")
)

missing_in_temp = (
    bronze_hashes
    .exceptAll(temp_hashes)
    .count()
)

extra_in_temp = (
    temp_hashes
    .exceptAll(bronze_hashes)
    .count()
)

print()
print("TWO-WAY HASH RECONCILIATION")
print(f"BRONZE -> TEMP missing = {missing_in_temp}")
print(f"TEMP -> BRONZE extra   = {extra_in_temp}")

if missing_in_temp != 0 or extra_in_temp != 0:
    raise RuntimeError(
        "BRONZE -> TEMP two-way hash reconciliation FAILED"
    )

print("TWO-WAY HASH STATUS = PASS")


# ============================================================
# VERIFY DATE RANGE
# ============================================================

range_check = (
    temp_df
    .agg(
        F.min("batch_date").alias("min_date"),
        F.max("batch_date").alias("max_date")
    )
    .collect()[0]
)

print()
print("TEMP RANGE")
print(f"MIN batch_date = {range_check['min_date']}")
print(f"MAX batch_date = {range_check['max_date']}")


print()
print("=" * 80)
print(
    f"SUCCESS: BRONZE -> TEMP {START_DATE} -> {END_DATE}"
)
print("=" * 80)


stage_df.unpersist()
temp_df.unpersist()

spark.stop()
