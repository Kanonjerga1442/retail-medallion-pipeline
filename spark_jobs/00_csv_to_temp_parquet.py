import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIG
# ============================================================

LANDING_DIR = "/opt/pipeline/data/landing_csv"

OUTPUT_DIR = "/opt/pipeline/data/temp_parquet/retail"

QUARANTINE_DIR = (
    "/opt/pipeline/data/quarantine/invalid_invoice_date"
)


# ============================================================
# FIND INPUT CSV
# ============================================================

csv_files = glob.glob(
    os.path.join(LANDING_DIR, "*.csv")
)

if len(csv_files) == 0:
    raise FileNotFoundError(
        f"No CSV file found in {LANDING_DIR}"
    )

if len(csv_files) > 1:
    raise RuntimeError(
        f"Expected exactly 1 CSV file, found: {csv_files}"
    )

INPUT_FILE = csv_files[0]

print("=" * 70)
print(f"INPUT FILE: {INPUT_FILE}")
print(f"OUTPUT:     {OUTPUT_DIR}")
print("=" * 70)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("online-retail-csv-to-temp-parquet")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# READ CSV
#
# IMPORTANT:
# inferSchema = false
#
# Bronze/staging should preserve source values as much as
# possible. We do not want Spark cleaning bad business data yet.
# ============================================================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .option("quote", '"')
    .option("escape", '"')
    .csv(INPUT_FILE)
)


print("\nSOURCE COLUMNS:")
print(df.columns)


# ============================================================
# NORMALIZE COLUMN NAMES
#
# Online Retail II exists in multiple versions:
#
# Invoice      -> InvoiceNo
# Price        -> UnitPrice
# Customer ID  -> CustomerID
#
# This is only technical schema normalization.
# No business cleaning happens here.
# ============================================================

rename_map = {
    "Invoice": "InvoiceNo",
    "Price": "UnitPrice",
    "Customer ID": "CustomerID"
}

for old_name, new_name in rename_map.items():

    if (
        old_name in df.columns
        and new_name not in df.columns
    ):
        df = df.withColumnRenamed(
            old_name,
            new_name
        )


required_columns = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country"
]


missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]


if missing_columns:

    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )


# ============================================================
# FORCE BUSINESS FIELDS TO STRING
#
# Dirty data is intentionally preserved here.
# ============================================================

df = df.select(
    *[
        F.col(column)
        .cast("string")
        .alias(column)

        for column in required_columns
    ]
)


# ============================================================
# PARSE InvoiceDate ONLY FOR TECHNICAL PARTITIONING
#
# Original InvoiceDate remains unchanged.
# ============================================================

parsed_timestamp = F.coalesce(

    F.expr(
        "try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')"
    ),

    F.expr(
        "try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm:ss')"
    ),

    F.expr(
        "try_to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss')"
    ),

    F.expr(
        "try_to_timestamp(InvoiceDate, 'yyyy-MM-dd H:mm:ss')"
    )
)


df = (
    df

    .withColumn(
        "_invoice_timestamp",
        parsed_timestamp
    )

    .withColumn(
        "_batch_date",
        F.to_date("_invoice_timestamp")
    )

    .withColumn(
        "_source_file",
        F.input_file_name()
    )

    .withColumn(
        "_converted_at",
        F.current_timestamp()
    )
)


# ============================================================
# CREATE ROW HASH
#
# Used later for:
# - reconciliation
# - duplicate checking
# - MINUS-style comparison
# ============================================================

hash_columns = [

    F.coalesce(
        F.col(column),
        F.lit("<NULL>")
    )

    for column in required_columns
]


df = df.withColumn(
    "_row_hash",

    F.sha2(
        F.concat_ws(
            "||",
            *hash_columns
        ),
        256
    )
)


# ============================================================
# COUNTS
# ============================================================

df.cache()

source_count = df.count()


valid_df = df.filter(
    F.col("_batch_date").isNotNull()
)


invalid_date_df = df.filter(
    F.col("_batch_date").isNull()
)


valid_count = valid_df.count()

invalid_count = invalid_date_df.count()


print("\n" + "=" * 70)

print(f"SOURCE ROWS       : {source_count}")
print(f"VALID DATE ROWS   : {valid_count}")
print(f"INVALID DATE ROWS : {invalid_count}")

print("=" * 70)


# ============================================================
# SHOW DIRTY DATA STATISTICS
#
# We only OBSERVE dirty data here.
# We do NOT clean it.
# ============================================================

null_customer_count = (
    df
    .filter(
        F.col("CustomerID").isNull()
        | (F.trim(F.col("CustomerID")) == "")
    )
    .count()
)


null_description_count = (
    df
    .filter(
        F.col("Description").isNull()
        | (F.trim(F.col("Description")) == "")
    )
    .count()
)


cancel_count = (
    df
    .filter(
        F.upper(
            F.trim(F.col("InvoiceNo"))
        ).startswith("C")
    )
    .count()
)


negative_quantity_count = (
    df
    .filter(
        F.expr(
            "try_cast(Quantity AS DOUBLE) < 0"
        )
    )
    .count()
)


print("\nDIRTY DATA PROFILE")
print("-" * 70)

print(
    f"Missing CustomerID : "
    f"{null_customer_count}"
)

print(
    f"Missing Description: "
    f"{null_description_count}"
)

print(
    f"Cancelled invoices : "
    f"{cancel_count}"
)

print(
    f"Negative Quantity  : "
    f"{negative_quantity_count}"
)


# ============================================================
# INVALID InvoiceDate -> QUARANTINE
# ============================================================

if invalid_count > 0:

    (
        invalid_date_df
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(QUARANTINE_DIR)
    )


# ============================================================
# VALID DATA -> TEMP PARQUET
#
# Partition by date.
# ============================================================

(
    valid_df
    .write
    .mode("overwrite")
    .partitionBy("_batch_date")
    .option("compression", "snappy")
    .parquet(OUTPUT_DIR)
)


# ============================================================
# VALIDATION: READ PARQUET BACK
# ============================================================

parquet_df = spark.read.parquet(
    OUTPUT_DIR
)


parquet_count = parquet_df.count()


date_stats = parquet_df.agg(

    F.min("_batch_date")
    .alias("min_date"),

    F.max("_batch_date")
    .alias("max_date"),

    F.countDistinct("_batch_date")
    .alias("number_of_dates")

).collect()[0]


print("\n" + "=" * 70)

print("PARQUET VALIDATION")

print("-" * 70)

print(
    f"CSV VALID ROW COUNT : {valid_count}"
)

print(
    f"PARQUET ROW COUNT   : {parquet_count}"
)

print(
    f"MIN DATE            : {date_stats['min_date']}"
)

print(
    f"MAX DATE            : {date_stats['max_date']}"
)

print(
    f"NUMBER OF DATES     : {date_stats['number_of_dates']}"
)


if valid_count != parquet_count:

    raise RuntimeError(
        "RECONCILIATION FAILED: "
        "CSV valid row count != Parquet row count"
    )


print("\nCOUNT RECONCILIATION: PASS")

print("=" * 70)


print("\nPARQUET SCHEMA")

parquet_df.printSchema()


spark.stop()
