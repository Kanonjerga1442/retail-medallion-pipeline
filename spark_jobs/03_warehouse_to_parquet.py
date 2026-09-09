import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


if len(sys.argv) not in {3, 4}:
    raise ValueError(
        "Usage: 03_warehouse_to_parquet.py START_DATE END_DATE [MODE]"
    )


START_DATE = sys.argv[1]
END_DATE = sys.argv[2]
MODE = sys.argv[3] if len(sys.argv) == 4 else "backfill"

start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

if start_dt > end_dt:
    raise ValueError("START_DATE must be <= END_DATE")


PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "warehouse")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

if not PGUSER or not PGPASSWORD:
    raise RuntimeError("Missing PostgreSQL credentials")


JDBC_URL = (
    f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
)


spark = (
    SparkSession.builder
    .appName(
        f"warehouse-parquet-export-{START_DATE}-{END_DATE}"
    )
    .config(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


jdbc_options = {
    "url": JDBC_URL,
    "user": PGUSER,
    "password": PGPASSWORD,
    "driver": "org.postgresql.Driver"
}


def read_query(query):

    return (
        spark.read
        .format("jdbc")
        .options(**jdbc_options)
        .option(
            "dbtable",
            f"({query}) AS export_query"
        )
        .load()
    )


def export_parquet(
    df,
    output_path,
    partition_column=None
):

    jvm = spark.sparkContext._gateway.jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    if partition_column:
        if MODE == "full_refresh":
            fs.delete(jvm.org.apache.hadoop.fs.Path(output_path), True)
        else:
            cursor = start_dt
            while cursor <= end_dt:
                partition_path = (
                    f"{output_path}/{partition_column}="
                    f"{cursor.strftime('%Y-%m-%d')}"
                )
                fs.delete(jvm.org.apache.hadoop.fs.Path(partition_path), True)
                cursor += timedelta(days=1)

    count = df.count()

    print("=" * 70)
    print(f"OUTPUT : {output_path}")
    print(f"ROWS   : {count}")

    if count == 0:
        print("NO DATA - SKIPPED")
        return

    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )

    if partition_column:
        writer = writer.partitionBy(partition_column)

    writer.parquet(output_path)

    print("EXPORT : PASS")


# ============================================================
# SILVER
# ============================================================

transactions = read_query(
    f"""
    SELECT *
    FROM silver.retail_transactions
    WHERE batch_date BETWEEN
          DATE '{START_DATE}'
      AND DATE '{END_DATE}'
    """
)

export_parquet(
    transactions,
    "/opt/pipeline/data/silver/retail_transactions",
    "batch_date"
)


rejects = read_query(
    f"""
    SELECT *
    FROM silver.retail_rejects
    WHERE batch_date BETWEEN
          DATE '{START_DATE}'
      AND DATE '{END_DATE}'
    """
)

export_parquet(
    rejects,
    "/opt/pipeline/data/silver/retail_rejects",
    "batch_date"
)


duplicates = read_query(
    f"""
    SELECT *
    FROM silver.retail_duplicates
    WHERE batch_date BETWEEN
          DATE '{START_DATE}'
      AND DATE '{END_DATE}'
    """
)

export_parquet(
    duplicates,
    "/opt/pipeline/data/silver/retail_duplicates",
    "batch_date"
)


# ============================================================
# GOLD STAR SCHEMA
# ============================================================

gold_tables = [
    (
        "gold.dim_date",
        "/opt/pipeline/data/gold/dim_date",
        "full_date",
        True
    ),
    (
        "gold.dim_product",
        "/opt/pipeline/data/gold/dim_product",
        None,
        False
    ),
    (
        "gold.dim_country",
        "/opt/pipeline/data/gold/dim_country",
        None,
        False
    ),
    (
        "gold.fact_sales",
        "/opt/pipeline/data/gold/fact_sales",
        "sales_date",
        True
    )
]


for table_name, output_path, date_column, filter_range in gold_tables:

    try:

        where_clause = ""
        if filter_range:
            where_clause = (
                f"WHERE {date_column} BETWEEN "
                f"DATE '{START_DATE}' AND DATE '{END_DATE}'"
            )

        df = read_query(f"SELECT * FROM {table_name} {where_clause}")

        export_parquet(
            df,
            output_path,
            date_column
        )

    except Exception as exc:

        print(
            f"SKIP {table_name}: "
            f"{str(exc).splitlines()[0]}"
        )


print("=" * 70)
print(
    f"WAREHOUSE PARQUET EXPORT COMPLETE "
    f"{START_DATE} -> {END_DATE}"
)
print("=" * 70)

spark.stop()
