import sys
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# ARGUMENTS
# ============================================================

if len(sys.argv) not in {3, 4}:
    raise ValueError(
        "Usage: 01_temp_parquet_to_bronze.py START_DATE END_DATE [MODE]"
    )

START_DATE = sys.argv[1]
END_DATE = sys.argv[2]
MODE = sys.argv[3] if len(sys.argv) == 4 else "backfill"

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
print(f"MODE       : {MODE}")
print("=" * 80)


# ============================================================
# READ ONLY SELECTED DATE RANGE
# ============================================================

try:
    source_df = spark.read.option("basePath", TEMP_BASE).parquet(TEMP_BASE)
except Exception:
    run_token = os.getenv("CDC_RUN_TOKEN")
    if not run_token:
        raise
    source_df = spark.read.parquet(
        f"/opt/pipeline/data/cdc/active/{run_token}/candidate_snapshot"
    )

source_df = source_df.filter(
    (F.col("_batch_date") >= F.lit(START_DATE).cast("date"))
    & (F.col("_batch_date") <= F.lit(END_DATE).cast("date"))
)

source_df.cache()

source_count = source_df.count()

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
# REPLACE THE EXACT TARGET PARTITION(S)
#
# Chỉ overwrite những invoice_date nằm trong dataframe.
# Không xóa Bronze history ngoài selected range.
# ============================================================

jvm = spark.sparkContext._gateway.jvm
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

if MODE == "full_refresh":
    fs.delete(jvm.org.apache.hadoop.fs.Path(BRONZE_BASE), True)
else:
    cursor = start_dt
    while cursor <= end_dt:
        partition_path = f"{BRONZE_BASE}/invoice_date={cursor.strftime('%Y-%m-%d')}"
        fs.delete(jvm.org.apache.hadoop.fs.Path(partition_path), True)
        cursor += timedelta(days=1)

if source_count > 0:
    (
        bronze_df.write
        .mode("append")
        .partitionBy("invoice_date")
        .option("compression", "snappy")
        .parquet(BRONZE_BASE)
    )

print("BRONZE WRITE COMPLETE")


# ============================================================
# READ TARGET RANGE BACK
# ============================================================

if fs.exists(jvm.org.apache.hadoop.fs.Path(BRONZE_BASE)):
    try:
        target_df = (
            spark.read.option("basePath", BRONZE_BASE).parquet(BRONZE_BASE)
            .filter(
                (F.col("invoice_date") >= F.lit(START_DATE).cast("date"))
                & (F.col("invoice_date") <= F.lit(END_DATE).cast("date"))
            )
        )
    except Exception:
        target_df = bronze_df.limit(0)
else:
    target_df = bronze_df.limit(0)

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

# Một batch tối đa 100 ngày nên collect tối đa 100 dòng thống kê là an toàn.
# Dùng cùng kết quả này để in log và kiểm tra, tránh chạy Spark action hai lần.
daily_rows = daily_check.orderBy("batch_date").collect()
for row in daily_rows:
    print(
        f"DATE={row['batch_date']} SOURCE={row['source_count']} "
        f"TARGET={row['target_count']} DIFFERENCE={row['difference']}"
    )

failed_days = sum(1 for row in daily_rows if row["difference"] != 0)

if failed_days != 0:
    raise RuntimeError(
        f"Partition-level reconciliation FAILED: {failed_days} day(s)"
    )

print("PER-DATE STATUS = PASS")


# ============================================================
# TWO-WAY ROW HASH RECONCILIATION
# ============================================================

source_hashes = source_df.groupBy(F.col("_row_hash").alias("row_hash")).count().withColumnRenamed(
    "count", "source_count"
)
target_hashes = target_df.groupBy(F.col("_row_hash").alias("row_hash")).count().withColumnRenamed(
    "count", "target_count"
)
hash_metrics = (
    source_hashes.join(target_hashes, ["row_hash"], "full")
    .fillna(0, subset=["source_count", "target_count"])
    .agg(
        F.coalesce(
            F.sum(F.greatest(F.col("source_count") - F.col("target_count"), F.lit(0))),
            F.lit(0),
        ).alias("missing_count"),
        F.coalesce(
            F.sum(F.greatest(F.col("target_count") - F.col("source_count"), F.lit(0))),
            F.lit(0),
        ).alias("extra_count"),
    )
    .collect()[0]
)
missing_in_bronze = hash_metrics["missing_count"]
extra_in_bronze = hash_metrics["extra_count"]

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
