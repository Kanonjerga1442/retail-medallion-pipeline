import json
import os
import re
import shutil
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


LANDING_DIR = os.getenv("LANDING_DIR", "/opt/pipeline/data/landing_csv")
TEMP_BASE = os.getenv("TEMP_BASE", "/opt/pipeline/data/temp_parquet/retail")
CDC_BASE = os.getenv("CDC_BASE", "/opt/pipeline/data/cdc")
STATE_BASE = f"{CDC_BASE}/state/retail_snapshot"
ACTIVE_BASE = f"{CDC_BASE}/active"
HISTORY_BASE = f"{CDC_BASE}/history"
QUARANTINE_BASE = os.getenv(
    "QUARANTINE_BASE", "/opt/pipeline/data/quarantine/invalid_invoice_date"
)

BUSINESS_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
]
IDENTITY_COLUMNS = ["InvoiceNo", "StockCode", "InvoiceDate", "CustomerID"]


def usage():
    raise ValueError(
        "Usage: 00_snapshot_cdc.py prepare|detect|commit "
        "full_refresh|incremental|backfill START_DATE END_DATE RUN_TOKEN"
    )


if len(sys.argv) != 6:
    usage()

ACTION, MODE, START_DATE, END_DATE, RUN_TOKEN = sys.argv[1:]
if ACTION not in {"prepare", "detect", "commit"}:
    usage()
if MODE not in {"full_refresh", "incremental", "backfill"}:
    usage()

start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
if start_dt > end_dt:
    raise ValueError("START_DATE must be <= END_DATE")
if not re.fullmatch(r"[A-Za-z0-9_.-]+", RUN_TOKEN):
    raise ValueError("RUN_TOKEN may contain only letters, numbers, dot, dash and underscore")

ACTIVE_RUN = f"{ACTIVE_BASE}/{RUN_TOKEN}"
MANIFEST_PATH = f"{ACTIVE_RUN}/manifest.json"
CANDIDATE_PATH = f"{ACTIVE_RUN}/candidate_snapshot"
EVENTS_PATH = f"{ACTIVE_RUN}/events"
STAGED_SOURCE_PATH = f"{ACTIVE_RUN}/source_snapshot"


def requested_dates():
    values = []
    cursor = start_dt
    while cursor <= end_dt:
        values.append(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)
    return values


def write_manifest(payload):
    manifest_dir = os.path.dirname(MANIFEST_PATH)
    os.makedirs(manifest_dir, exist_ok=True)
    temp_path = f"{MANIFEST_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.replace(temp_path, MANIFEST_PATH)


def fast_commit():
    """Commit Parquet bằng Arrow; không khởi động một Spark application mới."""
    if not os.path.exists(MANIFEST_PATH):
        raise FileNotFoundError(f"CDC manifest not found: {MANIFEST_PATH}")
    with open(MANIFEST_PATH, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    for value in requested_dates():
        partition_path = os.path.join(STATE_BASE, f"_batch_date={value}")
        if os.path.exists(partition_path):
            shutil.rmtree(partition_path)

    parquet_files = []
    if os.path.exists(CANDIDATE_PATH):
        for root, _, names in os.walk(CANDIDATE_PATH):
            parquet_files.extend(
                os.path.join(root, name) for name in names if name.endswith(".parquet")
            )
    if parquet_files:
        import pyarrow.parquet as pq

        candidate = pq.read_table(CANDIDATE_PATH)
        if candidate.num_rows:
            os.makedirs(STATE_BASE, exist_ok=True)
            pq.write_to_dataset(
                candidate,
                root_path=STATE_BASE,
                partition_cols=["_batch_date"],
                compression="snappy",
                # Spark writes timestamps at microsecond precision.  Arrow may
                # otherwise preserve nanoseconds when it rewrites the candidate,
                # which Spark's vectorized Parquet reader rejects on the next
                # incremental comparison (INT64 timestamp schema mismatch).
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
                basename_template=f"{RUN_TOKEN}-{{i}}.parquet",
                existing_data_behavior="overwrite_or_ignore",
            )

    history_path = f"{HISTORY_BASE}/run_token={RUN_TOKEN}"
    if os.path.exists(history_path):
        shutil.rmtree(history_path)
    if os.path.exists(EVENTS_PATH):
        os.makedirs(HISTORY_BASE, exist_ok=True)
        shutil.copytree(EVENTS_PATH, history_path)

    manifest["committed"] = True
    write_manifest(manifest)
    print("CDC STATE COMMITTED FAST " + json.dumps(manifest, sort_keys=True))


if ACTION == "commit":
    fast_commit()
    sys.exit(0)

spark = (
    SparkSession.builder
    .appName(f"retail-snapshot-cdc-{ACTION}-{RUN_TOKEN}")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

jvm = spark.sparkContext._gateway.jvm
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)


def path_exists(path):
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def delete_path(path):
    # All callers pass fixed children of /opt/pipeline/data/cdc or temp_parquet.
    fs.delete(jvm.org.apache.hadoop.fs.Path(path), True)


def read_csv_source():
    csv_files = [
        name for name in os.listdir(LANDING_DIR)
        if name.lower().endswith(".csv")
    ]
    if len(csv_files) != 1:
        raise RuntimeError(
            f"Expected exactly one CSV in {LANDING_DIR}; found {csv_files}"
        )
    input_file = os.path.join(LANDING_DIR, csv_files[0])
    source = (
        spark.read.option("header", "true").option("inferSchema", "false")
        .option("mode", "PERMISSIVE").option("quote", '"').option("escape", '"')
        .csv(input_file)
    )
    rename_map = {"Invoice": "InvoiceNo", "Price": "UnitPrice", "Customer ID": "CustomerID"}
    for old_name, new_name in rename_map.items():
        if old_name in source.columns and new_name not in source.columns:
            source = source.withColumnRenamed(old_name, new_name)
    missing = [column for column in BUSINESS_COLUMNS if column not in source.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    source = source.select(*[F.col(column).cast("string").alias(column) for column in BUSINESS_COLUMNS])
    parsed_timestamp = F.coalesce(
        F.expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')"),
        F.expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm:ss')"),
        F.expr("try_to_timestamp(InvoiceDate, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr("try_to_timestamp(InvoiceDate, 'yyyy-MM-dd H:mm:ss')"),
    )
    source = (
        source.withColumn("_invoice_timestamp", parsed_timestamp)
        .withColumn("_batch_date", F.to_date("_invoice_timestamp"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_converted_at", F.current_timestamp())
    )
    return source.filter(
        F.col("_batch_date").isNull()
        | (
            (F.col("_batch_date") >= F.lit(START_DATE).cast("date"))
            & (F.col("_batch_date") <= F.lit(END_DATE).cast("date"))
        )
    )


def normalize_source():
    staged_source = os.getenv("CDC_SOURCE_SNAPSHOT")
    if staged_source:
        if not path_exists(staged_source):
            raise FileNotFoundError(f"Prepared CDC source not found: {staged_source}")
        source = spark.read.option("basePath", staged_source).parquet(staged_source)
        source = selected(source)
        print(f"CDC SOURCE prepared_parquet={staged_source} range={START_DATE}..{END_DATE}")
    else:
        source = read_csv_source().filter(F.col("_batch_date").isNotNull())
        print(f"CDC SOURCE direct_csv range={START_DATE}..{END_DATE}")

    value_parts = [F.coalesce(F.col(column), F.lit("<NULL>")) for column in BUSINESS_COLUMNS]
    identity_parts = [F.coalesce(F.col(column), F.lit("<NULL>")) for column in IDENTITY_COLUMNS]
    source = (
        source.withColumn("_row_hash", F.sha2(F.concat_ws("||", *value_parts), 256))
        .withColumn("_identity_hash", F.sha2(F.concat_ws("||", *identity_parts), 256))
    )

    # The source has no line ID. This occurrence number makes repeated invoice lines
    # deterministic while the identity hash lets a value change be classified UPDATE.
    occurrence_window = Window.partitionBy("_identity_hash").orderBy("_row_hash")
    return (
        source.withColumn("_identity_occurrence", F.row_number().over(occurrence_window))
        .withColumn(
            "_record_key",
            F.sha2(F.concat_ws("||", "_identity_hash", F.col("_identity_occurrence").cast("string")), 256),
        )
    )


def empty_previous(schema):
    return spark.createDataFrame([], schema)


def selected(df):
    return df.filter(
        (F.col("_batch_date") >= F.lit(START_DATE).cast("date"))
        & (F.col("_batch_date") <= F.lit(END_DATE).cast("date"))
    )


def replace_date_partitions(df, base_path, dates):
    for value in dates:
        delete_path(f"{base_path}/_batch_date={value}")
    if df.limit(1).count() > 0:
        (
            df.repartition("_batch_date").write.mode("append").partitionBy("_batch_date")
            .option("compression", "snappy").parquet(base_path)
        )


if ACTION == "prepare":
    delete_path(ACTIVE_RUN)
    prepared = read_csv_source().cache()
    invalid_rows = prepared.filter(F.col("_batch_date").isNull())
    invalid_count = invalid_rows.count()
    delete_path(QUARANTINE_BASE)
    if invalid_count:
        invalid_rows.write.mode("overwrite").option("compression", "snappy").parquet(QUARANTINE_BASE)

    valid_rows = prepared.filter(F.col("_batch_date").isNotNull())
    valid_count = valid_rows.count()
    if valid_count == 0:
        raise RuntimeError(f"No valid source rows in range {START_DATE}..{END_DATE}")
    (
        valid_rows.repartition("_batch_date").write.mode("overwrite").partitionBy("_batch_date")
        .option("compression", "snappy").parquet(STAGED_SOURCE_PATH)
    )
    print(
        f"CDC SOURCE PREPARED path={STAGED_SOURCE_PATH} rows={valid_count} "
        f"invalid_rows={invalid_count} range={START_DATE}..{END_DATE}"
    )

elif ACTION == "detect":
    delete_path(ACTIVE_RUN)
    current = normalize_source().cache()

    if MODE in {"full_refresh", "backfill"} or not path_exists(STATE_BASE):
        previous = empty_previous(current.schema)
        baseline_created = True
    else:
        previous = selected(spark.read.parquet(STATE_BASE)).cache()
        baseline_created = False

    if MODE in {"full_refresh", "backfill"}:
        # Full refresh/backfill rebuild the selected 100-day range. Incremental
        # alone uses snapshot comparison to skip unchanged ranges.
        events = None
        counts = {}
        affected_dates = [str(row[0]) for row in current.select("_batch_date").distinct().orderBy("_batch_date").collect()]
    else:
        current_alias = current.alias("current")
        previous_alias = previous.alias("previous")
        joined = current_alias.join(
            previous_alias,
            F.col("current._record_key") == F.col("previous._record_key"),
            "full",
        )
        operation = (
            F.when(F.col("previous._record_key").isNull(), F.lit("INSERT"))
            .when(F.col("current._record_key").isNull(), F.lit("DELETE"))
            .when(F.col("current._row_hash") != F.col("previous._row_hash"), F.lit("UPDATE"))
        )
        events = (
            joined.withColumn("operation", operation).filter(F.col("operation").isNotNull())
            .select(
                F.coalesce(F.col("current._record_key"), F.col("previous._record_key")).alias("record_key"),
                "operation",
                F.col("previous._row_hash").alias("before_hash"),
                F.col("current._row_hash").alias("after_hash"),
                F.col("previous._batch_date").alias("before_date"),
                F.col("current._batch_date").alias("after_date"),
                F.to_json(F.struct(*[F.col(f"previous.{c}").alias(c) for c in BUSINESS_COLUMNS])).alias("before_payload"),
                F.to_json(F.struct(*[F.col(f"current.{c}").alias(c) for c in BUSINESS_COLUMNS])).alias("after_payload"),
                F.lit(RUN_TOKEN).alias("run_token"),
                F.current_timestamp().alias("detected_at"),
            )
        ).cache()
        counts = {row["operation"]: row["count"] for row in events.groupBy("operation").count().collect()}
        affected_date_rows = (
            events.select(F.col("before_date").alias("affected_date"))
            .union(events.select(F.col("after_date").alias("affected_date")))
            .filter(F.col("affected_date").isNotNull())
            .distinct()
            .orderBy("affected_date")
            .collect()
        )
        affected_dates = [str(row[0]) for row in affected_date_rows]

    current.write.mode("overwrite").option("compression", "snappy").parquet(CANDIDATE_PATH)
    if events is not None and events.limit(1).count() > 0:
        events.write.mode("overwrite").option("compression", "snappy").parquet(EVENTS_PATH)

    # Full/backfill stage toàn bộ window; incremental chỉ stage đúng ngày CDC
    # thay đổi. Partition bị DELETE hoàn toàn vẫn được xóa nhờ affected_dates.
    refresh_dates = (
        requested_dates()
        if MODE in {"full_refresh", "backfill"}
        else affected_dates
    )
    if refresh_dates:
        refresh_rows = current.filter(
            F.col("_batch_date").cast("string").isin(refresh_dates)
        )
        replace_date_partitions(refresh_rows, TEMP_BASE, refresh_dates)

    payload = {
        "run_token": RUN_TOKEN,
        "mode": MODE,
        "requested_start_date": START_DATE,
        "requested_end_date": END_DATE,
        "baseline_created": baseline_created,
        "affected_dates": affected_dates,
        "change_counts": {
            "insert": counts.get("INSERT", 0),
            "update": counts.get("UPDATE", 0),
            "delete": counts.get("DELETE", 0),
        },
        "candidate_rows": current.count(),
    }
    write_manifest(payload)
    print("CDC MANIFEST " + json.dumps(payload, sort_keys=True))

spark.stop()
