import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# ARGUMENTS
# ============================================================

if len(sys.argv) != 3:
    raise ValueError(
        "Usage: 01_temp_parquet_to_bronze.py START_DATE END_DATE"
    )

START_DATE = sys.argv[1]
END_DATE = sys.argv[2]

start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

if start_dt > end_dt:
    raise ValueError("START_DATE must be <= END_DATE")


# ============================================================
# PATHS
# ============================================================

TEMP_BASE = "/opt/pipeline/data/temp_parquet/retail"
BRONZE_BASE = "/opt/pipeline/data/bronze/retail"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName(f"temp-to-bronze-{START_DATE}-{END_DATE}")
    .config(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "16"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 80)
print("TEMP PARQUET -> BRONZE")
print(f"START_DATE : {START_DATE}")
print(f"END_DATE   : {END_DATE}")
print("=" * 80)


# ============================================================
# READ ONLY SELECTED DATE RANGE
# ============================================================

source_df = (
    spark.read
    .option("basePath", TEMP_BASE)
    .parquet(TEMP_BASE)
    .filter(
        (F.col("_batch_date") >= F.lit(START_DATE).cast("date"))
        &
        (F.col("_batch_date") <= F.lit(END_DATE).cast("date"))
    )
)

source_df.cache()

source_count = source_df.count()

if source_count == 0:
    raise RuntimeError(
        f"No TEMP Parquet data between {START_DATE} and {END_DATE}"
    )

print(f"SOURCE COUNT = {source_count}")


# ============================================================
# CREATE BRONZE DATAFRAME
# ============================================================

bronze_df = (
    source_df
    .withColumn(
        "_bronze_loaded_at",
        F.current_timestamp()
    )
    .withColumn(
        "_process_date",
        F.col("_batch_date").cast("date")
    )
    .withColumn(
        "_run_start_date",
        F.lit(START_DATE).cast("date")
    )
    .withColumn(
        "_run_end_date",
        F.lit(END_DATE).cast("date")
    )
    .withColumn(
        "_pipeline_name",
        F.lit("online_retail")
    )
    .withColumn(
        "invoice_date",
        F.col("_batch_date").cast("date")
    )
)


# ============================================================
# DYNAMIC PARTITION OVERWRITE
#
# Chỉ overwrite những invoice_date nằm trong dataframe.
# Không xóa Bronze history ngoài selected range.
# ============================================================

(
    bronze_df.write
    .mode("overwrite")
    .partitionBy("invoice_date")
    .option("compression", "snappy")
    .parquet(BRONZE_BASE)
)

print("BRONZE WRITE COMPLETE")


# ============================================================
# READ TARGET RANGE BACK
# ============================================================

target_df = (
    spark.read
    .option("basePath", BRONZE_BASE)
    .parquet(BRONZE_BASE)
    .filter(
        (F.col("invoice_date") >= F.lit(START_DATE).cast("date"))
        &
        (F.col("invoice_date") <= F.lit(END_DATE).cast("date"))
    )
)

target_df.cache()

target_count = target_df.count()


# ============================================================
# TOTAL COUNT RECONCILIATION
# ============================================================

print()
print("=" * 80)
print("TOTAL COUNT RECONCILIATION")
print(f"TEMP PARQUET = {source_count}")
print(f"BRONZE       = {target_count}")
print(f"DIFFERENCE   = {source_count - target_count}")

if source_count != target_count:
    raise RuntimeError(
        "TEMP -> BRONZE total count reconciliation FAILED"
    )

print("TOTAL COUNT STATUS = PASS")


# ============================================================
# PER-DATE RECONCILIATION
# ============================================================

source_daily = (
    source_df
    .groupBy(
        F.col("_batch_date").alias("batch_date")
    )
    .count()
    .withColumnRenamed("count", "source_count")
)

target_daily = (
    target_df
    .groupBy(
        F.col("invoice_date").alias("batch_date")
    )
    .count()
    .withColumnRenamed("count", "target_count")
)

daily_check = (
    source_daily
    .join(
        target_daily,
        ["batch_date"],
        "full"
    )
    .fillna(0)
    .withColumn(
        "difference",
        F.col("source_count") - F.col("target_count")
    )
)

print()
print("PER-DATE RECONCILIATION")

daily_check.orderBy("batch_date").show(
    1000,
    truncate=False
)

failed_days = (
    daily_check
    .filter(F.col("difference") != 0)
    .count()
)

if failed_days != 0:
    raise RuntimeError(
        f"Partition-level reconciliation FAILED: {failed_days} day(s)"
    )

print("PER-DATE STATUS = PASS")


# ============================================================
# TWO-WAY ROW HASH RECONCILIATION
# ============================================================

source_hashes = source_df.select(
    F.col("_row_hash").alias("row_hash")
)

target_hashes = target_df.select(
    F.col("_row_hash").alias("row_hash")
)

missing_in_bronze = (
    source_hashes
    .exceptAll(target_hashes)
    .count()
)

extra_in_bronze = (
    target_hashes
    .exceptAll(source_hashes)
    .count()
)

print()
print("TWO-WAY HASH RECONCILIATION")
print(f"TEMP -> BRONZE missing = {missing_in_bronze}")
print(f"BRONZE -> TEMP extra   = {extra_in_bronze}")

if missing_in_bronze != 0 or extra_in_bronze != 0:
    raise RuntimeError(
        "TEMP -> BRONZE two-way hash reconciliation FAILED"
    )

print("TWO-WAY HASH STATUS = PASS")

print()
print("=" * 80)
print(
    f"SUCCESS: TEMP -> BRONZE {START_DATE} -> {END_DATE}"
)
print("=" * 80)


source_df.unpersist()
target_df.unpersist()

spark.stop()
