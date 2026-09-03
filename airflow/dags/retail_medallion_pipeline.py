from __future__ import annotations

import os
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


@dataclass(frozen=True)
class PipelineJob:
    """Một đơn vị chạy: ID duy nhất, run_order nhỏ chạy trước."""

    job_id: int
    table_group: str
    run_order: int
    table_name: str
    command_factory: Callable[[str, str], list[str]]
    cwd: str | None = None


def spark_command(script: str, start: str, end: str, jdbc: bool = False) -> list[str]:
    command = [SPARK_SUBMIT, "--master", SPARK_MASTER]
    if jdbc:
        command += ["--jars", POSTGRES_JAR, "--driver-class-path", POSTGRES_JAR]
    return command + [f"{SPARK_JOBS_DIR}/{script}", start, end]


def dbt_command(action: str, selector: str, start: str, end: str) -> list[str]:
    return [
        AIRFLOW_PYTHON, DBT_EXECUTABLE, action, "--project-dir", DBT_PROJECT,
        "--profiles-dir", DBT_PROFILES, "--select", selector,
        "--vars", f"{{start_date: {start}, end_date: {end}}}",
    ]


# Nguồn cấu hình duy nhất của pipeline. Các ID lớn hơn luôn chạy sau.
# table_group giúp các bước của cùng một bảng/layer nằm cạnh nhau trong log.
PIPELINE_JOBS = (
    PipelineJob(100, "bronze.retail", 10, "bronze.retail", lambda s, e: spark_command("01_temp_parquet_to_bronze.py", s, e)),
    PipelineJob(200, "temp.retail_batch", 10, "temp.retail_batch", lambda s, e: spark_command("02_bronze_to_temp_sql.py", s, e, True)),
    PipelineJob(300, "silver.retail", 10, "silver.common", lambda s, e: dbt_command("run", "int_retail_classified", s, e), DBT_PROJECT),
    PipelineJob(310, "silver.retail", 20, "silver.retail_rejects", lambda s, e: dbt_command("run", "retail_rejects", s, e), DBT_PROJECT),
    PipelineJob(320, "silver.retail", 30, "silver.retail_duplicates", lambda s, e: dbt_command("run", "retail_duplicates", s, e), DBT_PROJECT),
    PipelineJob(330, "silver.retail", 40, "silver.retail_transactions", lambda s, e: dbt_command("run", "retail_transactions", s, e), DBT_PROJECT),
    PipelineJob(340, "silver.retail", 50, "silver.validation", lambda s, e: dbt_command("test", "path:tests", s, e), DBT_PROJECT),
    PipelineJob(400, "gold.dimensions", 10, "gold.dim_date", lambda s, e: dbt_command("run", "dim_date", s, e), DBT_PROJECT),
    PipelineJob(410, "gold.dimensions", 20, "gold.dim_product", lambda s, e: dbt_command("run", "dim_product", s, e), DBT_PROJECT),
    PipelineJob(420, "gold.dimensions", 30, "gold.dim_country", lambda s, e: dbt_command("run", "dim_country", s, e), DBT_PROJECT),
    PipelineJob(500, "gold.facts", 10, "gold.fact_sales", lambda s, e: dbt_command("run", "fact_sales", s, e), DBT_PROJECT),
    PipelineJob(510, "gold.facts", 20, "gold.validation", lambda s, e: dbt_command("test", "path:models/gold", s, e), DBT_PROJECT),
    PipelineJob(600, "export", 10, "silver_and_gold_parquet", lambda s, e: spark_command("03_warehouse_to_parquet.py", s, e, True)),
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


def create_batches(start: date, end: date, mode: str) -> list[tuple[date, date]]:
    """Create internal log batches without creating extra Airflow tasks."""

    if mode == "incremental":
        return [(start, end)]

    batches: list[tuple[date, date]] = []
    batch_start = start
    while batch_start <= end:
        batch_end = min(batch_start + timedelta(days=BATCH_DAYS - 1), end)
        batches.append((batch_start, batch_end))
        batch_start = batch_end + timedelta(days=1)
    return batches


with DAG(
    dag_id="retail_medallion_pipeline",
    description="ID-driven Retail Medallion pipeline with ordered table groups",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["retail", "medallion", "id-driven", "star-schema"],
    params={
        "mode": Param("incremental", type="string", enum=["incremental", "backfill"]),
        "start_date": Param("2011-05-05", type="string", format="date"),
        "end_date": Param("2011-05-06", type="string", format="date"),
        "batch_days": Param(
            BATCH_DAYS,
            type="integer",
            minimum=BATCH_DAYS,
            maximum=BATCH_DAYS,
            title="Fixed Batch Days",
            description="Fixed at 100 days so every backfill batch has a predictable size.",
        ),
    },
) as dag:

    @task(task_id="run_pipeline")
    def run_pipeline() -> None:
        params = get_current_context()["params"]
        mode = str(params["mode"]).strip().lower()
        start = date.fromisoformat(str(params["start_date"]))
        end = date.fromisoformat(str(params["end_date"]))

        if start > end:
            raise ValueError("start_date cannot be after end_date")
        if mode not in {"incremental", "backfill"}:
            raise ValueError(f"Unsupported mode: {mode}")

        selected_days = (end - start).days + 1
        if mode == "incremental" and selected_days > BATCH_DAYS:
            raise ValueError(
                f"Incremental mode accepts at most {BATCH_DAYS} days; got {selected_days}. "
                "Use backfill for historical ranges."
            )
        print(
            f"VALID CONFIG mode={mode} range={start}..{end} days={selected_days} "
            f"batch_days={BATCH_DAYS}"
        )

        batches = create_batches(start, end, mode)
        total_batches = len(batches)
        pipeline_started = time.monotonic()
        print(f"PIPELINE START total_batches={total_batches}")

        for batch_number, (batch_start, batch_end) in enumerate(batches, start=1):
            start_str, end_str = batch_start.isoformat(), batch_end.isoformat()
            batch_started = time.monotonic()
            print("#" * 100)
            print(
                f"BATCH START batch={batch_number}/{total_batches} "
                f"range={start_str}..{end_str} "
                f"days={(batch_end - batch_start).days + 1}"
            )
            print("#" * 100)

            for job in ordered_jobs():
                job_started = time.monotonic()
                print("=" * 100)
                print(
                    f"JOB START batch={batch_number}/{total_batches} job_id={job.job_id} "
                    f"priority={job.run_order} group={job.table_group} "
                    f"table={job.table_name} range={start_str}..{end_str}"
                )
                print("=" * 100)
                try:
                    run_command(job.command_factory(start_str, end_str), job.cwd)
                except Exception:
                    print("!" * 100)
                    print(
                        f"JOB FAILED batch={batch_number}/{total_batches} job_id={job.job_id} "
                        f"table={job.table_name} range={start_str}..{end_str} "
                        f"elapsed_seconds={time.monotonic() - job_started:.2f}"
                    )
                    print("!" * 100)
                    raise
                print(
                    f"JOB SUCCESS batch={batch_number}/{total_batches} job_id={job.job_id} "
                    f"table={job.table_name} "
                    f"elapsed_seconds={time.monotonic() - job_started:.2f}"
                )

            print(
                f"BATCH SUCCESS batch={batch_number}/{total_batches} "
                f"range={start_str}..{end_str} "
                f"elapsed_seconds={time.monotonic() - batch_started:.2f}"
            )

        print(
            f"PIPELINE SUCCESS total_batches={total_batches} "
            f"elapsed_seconds={time.monotonic() - pipeline_started:.2f}"
        )

    run_pipeline()
