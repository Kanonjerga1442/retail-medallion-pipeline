from __future__ import annotations

import csv
import os
import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from airflow.sdk import DAG, Param, get_current_context, task

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS_DIR = "/opt/pipeline/spark_jobs"
POSTGRES_JAR = f"{SPARK_JOBS_DIR}/lib/postgresql-42.7.7.jar"
DBT_PROJECT = "/opt/pipeline/dbt_retail"
DBT_PROFILES = DBT_PROJECT
AIRFLOW_PYTHON = "/home/airflow/.local/bin/python"
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"
BATCH_DAYS = 100
LANDING_DIR = "/opt/pipeline/data/landing_csv"
SPARK_RESOURCE_ARGS = [
    "--driver-memory", "1g",
    "--executor-memory", "2g",
    "--executor-cores", "4",
]

FULL_REFRESH_PATHS = (
    "/opt/pipeline/data/temp_parquet/retail",
    "/opt/pipeline/data/bronze/retail",
    "/opt/pipeline/data/silver/retail_transactions",
    "/opt/pipeline/data/silver/retail_rejects",
    "/opt/pipeline/data/silver/retail_duplicates",
    "/opt/pipeline/data/gold/dim_date",
    "/opt/pipeline/data/gold/dim_product",
    "/opt/pipeline/data/gold/dim_country",
    "/opt/pipeline/data/gold/fact_sales",
    "/opt/pipeline/data/cdc/state/retail_snapshot",
    "/opt/pipeline/data/cdc/active",
)

FULL_REFRESH_TABLES = (
    "temp.retail_batch",
    "silver.retail_rejects",
    "silver.retail_duplicates",
    "silver.retail_transactions",
    "gold.fact_sales",
    "gold.dim_date",
    "gold.dim_product",
    "gold.dim_country",
)


@dataclass(frozen=True)
class PipelineJob:
    """Một đơn vị chạy: ID duy nhất, run_order nhỏ chạy trước."""

    job_id: int
    table_group: str
    run_order: int
    table_name: str
    command_factory: Callable[[str, str, str], list[str]]
    cwd: str | None = None


def spark_command(script: str, start: str, end: str, mode: str, jdbc: bool = False) -> list[str]:
    command = [SPARK_SUBMIT, "--master", SPARK_MASTER, *SPARK_RESOURCE_ARGS]
    if jdbc:
        command += ["--jars", POSTGRES_JAR, "--driver-class-path", POSTGRES_JAR]
    return command + [f"{SPARK_JOBS_DIR}/{script}", start, end, mode]


def dbt_command(action: str, selector: str, start: str, end: str, mode: str) -> list[str]:
    command = [
        AIRFLOW_PYTHON, DBT_EXECUTABLE, action, "--project-dir", DBT_PROJECT,
        "--profiles-dir", DBT_PROFILES, "--select", selector,
        "--vars", f"{{start_date: {start}, end_date: {end}, pipeline_mode: {mode}}}",
    ]
    return command


def cdc_command(action: str, mode: str, start: str, end: str, run_token: str) -> list[str]:
    script_path = f"{SPARK_JOBS_DIR}/00_snapshot_cdc.py"
    if action == "commit":
        return [AIRFLOW_PYTHON, script_path, action, mode, start, end, run_token]
    return [
        SPARK_SUBMIT, "--master", SPARK_MASTER, *SPARK_RESOURCE_ARGS,
        script_path,
        action, mode, start, end, run_token,
    ]


# Nguồn cấu hình duy nhất của pipeline. Các ID lớn hơn luôn chạy sau.
# table_group giúp các bước của cùng một bảng/layer nằm cạnh nhau trong log.
PIPELINE_JOBS = (
    PipelineJob(100, "bronze.retail", 10, "bronze.retail", lambda s, e, m: spark_command("01_temp_parquet_to_bronze.py", s, e, m)),
    PipelineJob(200, "temp.retail_batch", 10, "temp.retail_batch", lambda s, e, m: spark_command("02_bronze_to_temp_sql.py", s, e, m, True)),
    PipelineJob(300, "silver.retail", 10, "silver.common", lambda s, e, m: dbt_command("run", "int_retail_classified", s, e, m), DBT_PROJECT),
    PipelineJob(310, "silver.retail", 20, "silver.retail_rejects", lambda s, e, m: dbt_command("run", "retail_rejects", s, e, m), DBT_PROJECT),
    PipelineJob(320, "silver.retail", 30, "silver.retail_duplicates", lambda s, e, m: dbt_command("run", "retail_duplicates", s, e, m), DBT_PROJECT),
    PipelineJob(330, "silver.retail", 40, "silver.retail_transactions", lambda s, e, m: dbt_command("run", "retail_transactions", s, e, m), DBT_PROJECT),
    PipelineJob(340, "silver.retail", 50, "silver.validation", lambda s, e, m: dbt_command("test", "path:tests", s, e, m), DBT_PROJECT),
    PipelineJob(400, "gold.dimensions", 10, "gold.dim_date", lambda s, e, m: dbt_command("run", "dim_date", s, e, m), DBT_PROJECT),
    PipelineJob(410, "gold.dimensions", 20, "gold.dim_product", lambda s, e, m: dbt_command("run", "dim_product", s, e, m), DBT_PROJECT),
    PipelineJob(420, "gold.dimensions", 30, "gold.dim_country", lambda s, e, m: dbt_command("run", "dim_country", s, e, m), DBT_PROJECT),
    PipelineJob(500, "gold.facts", 10, "gold.fact_sales", lambda s, e, m: dbt_command("run", "fact_sales", s, e, m), DBT_PROJECT),
    PipelineJob(510, "gold.facts", 20, "gold.validation", lambda s, e, m: dbt_command("test", "path:models/gold", s, e, m), DBT_PROJECT),
)

EXPORT_JOB = PipelineJob(
    600,
    "export",
    10,
    "silver_and_gold_parquet",
    lambda s, e, m: spark_command("03_warehouse_to_parquet.py", s, e, m, True),
)


def ordered_jobs() -> list[PipelineJob]:
    ids = [job.job_id for job in PIPELINE_JOBS]
    if len(ids) != len(set(ids)):
        raise ValueError("PIPELINE_JOBS contains duplicate job_id")
    return sorted(PIPELINE_JOBS, key=lambda job: (job.job_id, job.run_order))


def run_command(command: list[str], cwd: str | None = None) -> None:
    env = os.environ.copy()
    env.setdefault("DBT_LOG_PATH", "/tmp/dbt_logs")
    env.setdefault("DBT_TARGET_PATH", "/tmp/dbt_target")
    env.setdefault("DBT_PROFILES_DIR", DBT_PROFILES)
    os.makedirs(env["DBT_LOG_PATH"], exist_ok=True)
    os.makedirs(env["DBT_TARGET_PATH"], exist_ok=True)
    print("COMMAND:", " ".join(command))
    subprocess.run(command, cwd=cwd, env=env, check=True)


def create_batches(start: date, end: date) -> list[tuple[date, date]]:
    """Chia mọi mode thành các đoạn tối đa 100 ngày trong cùng một task Airflow."""
    batches: list[tuple[date, date]] = []
    batch_start = start
    while batch_start <= end:
        batch_end = min(batch_start + timedelta(days=BATCH_DAYS - 1), end)
        batches.append((batch_start, batch_end))
        batch_start = batch_end + timedelta(days=1)
    return batches


def consecutive_date_ranges(values: list[date]) -> list[tuple[date, date]]:
    """Gom ngày CDC liên tiếp; không mở rộng sang ngày không thay đổi."""
    if not values:
        return []
    ordered = sorted(set(values))
    ranges: list[tuple[date, date]] = []
    range_start = ordered[0]
    range_end = ordered[0]
    for value in ordered[1:]:
        if value == range_end + timedelta(days=1):
            range_end = value
            continue
        ranges.append((range_start, range_end))
        range_start = value
        range_end = value
    ranges.append((range_start, range_end))
    return ranges


def parse_invoice_date(value: str) -> date | None:
    value = value.strip()
    for date_format in (
        "%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            continue
    return None


def source_date_range() -> tuple[date, date, int, int]:
    """Đọc nhẹ cột ngày để tránh khởi động Spark với range 1900..9999."""
    csv_files = sorted(
        os.path.join(LANDING_DIR, name)
        for name in os.listdir(LANDING_DIR)
        if name.lower().endswith(".csv")
    )
    if len(csv_files) != 1:
        raise RuntimeError(f"Expected exactly one CSV in {LANDING_DIR}; found {csv_files}")

    min_date: date | None = None
    max_date: date | None = None
    valid_rows = 0
    invalid_rows = 0
    with open(csv_files[0], "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        date_column = next(
            (name for name in ("InvoiceDate", "Invoice Date") if name in (reader.fieldnames or [])),
            None,
        )
        if not date_column:
            raise ValueError(f"InvoiceDate column not found; columns={reader.fieldnames}")
        for row in reader:
            parsed = parse_invoice_date(row.get(date_column) or "")
            if parsed is None:
                invalid_rows += 1
                continue
            valid_rows += 1
            min_date = parsed if min_date is None else min(min_date, parsed)
            max_date = parsed if max_date is None else max(max_date, parsed)

    if min_date is None or max_date is None:
        raise RuntimeError("Source CSV contains no valid InvoiceDate values")
    return min_date, max_date, valid_rows, invalid_rows


def reset_full_refresh() -> None:
    """Reset một lần trước batch 1; các batch sau chỉ replace range của mình."""
    print("FULL REFRESH RESET START")
    for path in FULL_REFRESH_PATHS:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"FULL REFRESH RESET PATH removed={path}")

    import psycopg2

    connection = psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "warehouse"),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
    )
    try:
        with connection.cursor() as cursor:
            existing_tables: list[str] = []
            for table_name in FULL_REFRESH_TABLES:
                cursor.execute("SELECT to_regclass(%s)", (table_name,))
                if cursor.fetchone()[0] is not None:
                    existing_tables.append(table_name)
            if existing_tables:
                cursor.execute("TRUNCATE TABLE " + ", ".join(existing_tables))
                print("FULL REFRESH RESET TABLES truncated=" + ",".join(existing_tables))
        connection.commit()
    finally:
        connection.close()
    print("FULL REFRESH RESET SUCCESS")


with DAG(
    dag_id="retail_medallion_pipeline",
    description="Retail Medallion pipeline with full refresh and snapshot-diff CDC",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["retail", "medallion", "id-driven", "star-schema"],
    params={
        "mode": Param("incremental", type="string", enum=["full_refresh", "incremental", "backfill"]),
        "backfill_start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            title="Backfill Start Date",
            description="Required only when mode=backfill. Leave empty for incremental and full_refresh.",
        ),
        "backfill_end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            title="Backfill End Date",
            description="Required only when mode=backfill. Leave empty for incremental and full_refresh.",
        ),
        "batch_days": Param(
            BATCH_DAYS,
            type="integer",
            minimum=BATCH_DAYS,
            maximum=BATCH_DAYS,
            title="Fixed Batch Days",
            description="Fixed at 100 days for full refresh, incremental and backfill.",
        ),
    },
) as dag:

    @task(task_id="run_pipeline")
    def run_pipeline() -> None:
        params = get_current_context()["params"]
        mode = str(params["mode"]).strip().lower()
        if mode not in {"full_refresh", "incremental", "backfill"}:
            raise ValueError(f"Unsupported mode: {mode}")

        raw_start = params.get("backfill_start_date")
        raw_end = params.get("backfill_end_date")
        if mode == "backfill":
            if not raw_start or not raw_end:
                raise ValueError(
                    "backfill_start_date and backfill_end_date are required "
                    "when mode=backfill"
                )
            start = date.fromisoformat(str(raw_start))
            end = date.fromisoformat(str(raw_end))
            if start > end:
                raise ValueError("backfill_start_date cannot be after backfill_end_date")
            selected_days = (end - start).days + 1
            print(
                f"VALID CONFIG mode=backfill range={start}..{end} "
                f"days={selected_days} batch_days={BATCH_DAYS}"
            )
        else:
            if raw_start or raw_end:
                raise ValueError(
                    f"Date range is not allowed when mode={mode}; "
                    "leave Backfill Start/End Date empty"
                )
            start, end, valid_rows, invalid_rows = source_date_range()
            print(
                f"VALID CONFIG mode={mode} source_range={start}..{end} "
                f"valid_source_rows={valid_rows} invalid_source_rows={invalid_rows}"
            )

        requested_batches = create_batches(start, end)
        total_batches = len(requested_batches)
        pipeline_started = time.monotonic()
        airflow_run_id = str(get_current_context().get("run_id", "manual"))
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", airflow_run_id)[-80:]
        total_days = (end - start).days + 1
        print("=" * 100)
        print(
            f"PIPELINE PLAN mode={mode} range={start}..{end} total_days={total_days} "
            f"batch_days={BATCH_DAYS} total_batches={total_batches}"
        )
        print("=" * 100)

        if mode == "full_refresh":
            reset_full_refresh()

        # Full refresh đã reset đúng một lần. Truyền backfill xuống các job để
        # mỗi batch chỉ replace 100 ngày, không xóa thành quả của batch trước.
        execution_mode = "backfill" if mode == "full_refresh" else mode
        pipeline_token = f"{safe_run_id}_source"
        staged_source_path = f"/opt/pipeline/data/cdc/active/{pipeline_token}/source_snapshot"
        print(
            f"SOURCE PREPARE START mode={mode} range={start}..{end} "
            f"target={staged_source_path}"
        )
        run_command(cdc_command("prepare", mode, start.isoformat(), end.isoformat(), pipeline_token))
        os.environ["CDC_SOURCE_SNAPSHOT"] = staged_source_path
        print(f"SOURCE PREPARE SUCCESS target={staged_source_path}")

        processed_any_batch = False
        export_start: date | None = None
        export_end: date | None = None

        for batch_number, (batch_start, batch_end) in enumerate(requested_batches, start=1):
            start_str, end_str = batch_start.isoformat(), batch_end.isoformat()
            batch_started = time.monotonic()
            run_token = f"{safe_run_id}_{start_str}_{end_str}"
            batch_days = (batch_end - batch_start).days + 1
            print("#" * 100)
            print(
                f"BATCH START batch={batch_number}/{total_batches} mode={mode} "
                f"range={start_str}..{end_str} days={batch_days}"
            )
            print(f"CDC START batch={batch_number}/{total_batches} token={run_token}")
            run_command(cdc_command("detect", mode, start_str, end_str, run_token))
            manifest_path = f"/opt/pipeline/data/cdc/active/{run_token}/manifest.json"
            with open(manifest_path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            affected_dates = [date.fromisoformat(value) for value in manifest["affected_dates"]]

            should_process = mode in {"full_refresh", "backfill"} or bool(affected_dates)
            work_ranges = (
                [(batch_start, batch_end)]
                if mode in {"full_refresh", "backfill"}
                else consecutive_date_ranges(affected_dates)
            )

            os.environ["CDC_RUN_TOKEN"] = run_token

            print(
                f"CDC SUCCESS batch={batch_number}/{total_batches} mode={mode} "
                f"range={start_str}..{end_str} candidate_rows={manifest['candidate_rows']} "
                f"changes={manifest['change_counts']} affected_dates={len(affected_dates)}"
            )

            if should_process:
                processed_any_batch = True
                for window_number, (work_start, work_end) in enumerate(work_ranges, start=1):
                    export_start = work_start if export_start is None else min(export_start, work_start)
                    export_end = work_end if export_end is None else max(export_end, work_end)
                    work_start_str = work_start.isoformat()
                    work_end_str = work_end.isoformat()
                    print(
                        f"CHANGE WINDOW START batch={batch_number}/{total_batches} "
                        f"window={window_number}/{len(work_ranges)} "
                        f"range={work_start_str}..{work_end_str}"
                    )
                    for job in ordered_jobs():
                        job_started = time.monotonic()
                        print(
                            f"JOB START batch={batch_number}/{total_batches} "
                            f"window={window_number}/{len(work_ranges)} job_id={job.job_id} "
                            f"group={job.table_group} priority={job.run_order} table={job.table_name} "
                            f"range={work_start_str}..{work_end_str}"
                        )
                        try:
                            run_command(
                                job.command_factory(work_start_str, work_end_str, execution_mode),
                                job.cwd,
                            )
                        except Exception:
                            print(
                                f"JOB FAILED batch={batch_number}/{total_batches} "
                                f"window={window_number}/{len(work_ranges)} job_id={job.job_id} "
                                f"group={job.table_group} priority={job.run_order} table={job.table_name} "
                                f"range={work_start_str}..{work_end_str} "
                                f"elapsed_seconds={time.monotonic() - job_started:.2f}"
                            )
                            raise
                        print(
                            f"JOB SUCCESS batch={batch_number}/{total_batches} "
                            f"window={window_number}/{len(work_ranges)} job_id={job.job_id} "
                            f"table={job.table_name} "
                            f"elapsed_seconds={time.monotonic() - job_started:.2f}"
                        )
                    print(
                        f"CHANGE WINDOW SUCCESS batch={batch_number}/{total_batches} "
                        f"window={window_number}/{len(work_ranges)} "
                        f"range={work_start_str}..{work_end_str}"
                    )
            else:
                print(
                    f"BATCH NO CHANGES batch={batch_number}/{total_batches} "
                    f"range={start_str}..{end_str}; downstream jobs skipped"
                )

            # Commit sau khi downstream thành công. Incremental không đổi giữ
            # nguyên state cũ, tránh đọc/ghi lại Parquet không cần thiết.
            if should_process:
                commit_started = time.monotonic()
                run_command(cdc_command("commit", mode, start_str, end_str, run_token))
                print(
                    f"CDC COMMIT SUCCESS batch={batch_number}/{total_batches} "
                    f"elapsed_seconds={time.monotonic() - commit_started:.2f}"
                )
            else:
                print(f"CDC COMMIT SKIPPED batch={batch_number}/{total_batches} reason=no_changes")

            print(
                f"BATCH SUCCESS batch={batch_number}/{total_batches} "
                f"range={start_str}..{end_str} "
                f"elapsed_seconds={time.monotonic() - batch_started:.2f}"
            )

        if processed_any_batch:
            if export_start is None or export_end is None:
                raise RuntimeError("Processed batches exist but final export range is missing")
            export_started = time.monotonic()
            print(
                f"FINAL JOB START job_id={EXPORT_JOB.job_id} table={EXPORT_JOB.table_name} "
                f"range={export_start}..{export_end}"
            )
            try:
                run_command(
                    EXPORT_JOB.command_factory(
                        export_start.isoformat(), export_end.isoformat(), execution_mode
                    ),
                    EXPORT_JOB.cwd,
                )
            except Exception:
                print(
                    f"FINAL JOB FAILED job_id={EXPORT_JOB.job_id} table={EXPORT_JOB.table_name} "
                    f"elapsed_seconds={time.monotonic() - export_started:.2f}"
                )
                raise
            print(
                f"FINAL JOB SUCCESS job_id={EXPORT_JOB.job_id} table={EXPORT_JOB.table_name} "
                f"elapsed_seconds={time.monotonic() - export_started:.2f}"
            )
        else:
            print("FINAL JOB SKIPPED job_id=600 reason=no_changed_batches")

        staged_source_run = os.path.dirname(staged_source_path)
        if os.path.exists(staged_source_run):
            shutil.rmtree(staged_source_run)
            print(f"SOURCE PREPARE CLEANUP removed={staged_source_run}")

        print(
            f"PIPELINE SUCCESS mode={mode} source_batches={total_batches} "
            f"elapsed_seconds={time.monotonic() - pipeline_started:.2f}"
        )

    run_pipeline()
