# Incremental CDC validation

This document reproduces the one-row snapshot-diff CDC test used for the portfolio. Run the commands from the repository root in PowerShell. A successful full refresh must exist before this test so `data/cdc/state/retail_snapshot` is a complete baseline.

## Tested behavior

The source row is identified by invoice `489434` and stock code `85048`. Only `Description` is changed; invoice, product, timestamp, customer, quantity and price remain unchanged. The expected CDC result is therefore exactly one `UPDATE`, not an `INSERT` plus a `DELETE`.

Validated Airflow evidence from run `portfolio_incremental_update_20260909`, retry 2:

```text
CDC MANIFEST {"affected_dates":["2009-12-01"],"baseline_created":false,
"change_counts":{"delete":0,"insert":0,"update":1}}
CHANGE WINDOW START batch=1/8 window=1/1 range=2009-12-01..2009-12-01
CHANGE WINDOW SUCCESS batch=1/8 window=1/1 range=2009-12-01..2009-12-01
```

The outer batch remains 100 calendar days for observability. `affected_dates` is converted to one or more consecutive change windows, and only those windows execute Bronze, Silver and Gold jobs. Other batches log `BATCH NO CHANGES` and skip downstream transformations.

The source was then restored and validated with run `portfolio_incremental_restore_verified_20260909`. It detected the reverse change as one `UPDATE`, processed only `2009-12-01`, skipped batches 2 through 8, and completed successfully in 855.75 seconds. The restored source SHA-256 matched the pre-test backup:

```text
32569A66F3842A82B0D8C4D63B263C5D98A76BDE5D1F65C6C01BF457E541D3A9
```

## 1. Back up and change exactly one row

```powershell
New-Item -ItemType Directory -Force data/test_backups | Out-Null
Copy-Item -LiteralPath data/landing_csv/online_retail_II.csv -Destination data/test_backups/online_retail_II.before_incremental_test.csv

$sourcePath = (Resolve-Path data/landing_csv/online_retail_II.csv).Path
$encoding = [System.Text.Encoding]::Latin1
$oldValue = '489434,85048,15CM CHRISTMAS GLASS BALL 20 LIGHTS,12,2009-12-01 07:45:00,6.95,13085.0,United Kingdom'
$newValue = '489434,85048,15CM CHRISTMAS GLASS BALL 20 LIGHTS CDC_TEST_UPDATE,12,2009-12-01 07:45:00,6.95,13085.0,United Kingdom'
$content = [System.IO.File]::ReadAllText($sourcePath, $encoding)
if (([regex]::Matches($content, [regex]::Escape($oldValue))).Count -ne 1) { throw 'Expected exactly one matching source row' }
[System.IO.File]::WriteAllText($sourcePath, $content.Replace($oldValue, $newValue), $encoding)
```

## 2. Trigger incremental and inspect CDC logs

```powershell
$runId = 'portfolio_incremental_update_' + (Get-Date -Format yyyyMMdd_HHmmss)
$airflowConf = "{`"mode`":`"incremental`"}"
docker compose exec -T airflow /home/airflow/.local/bin/python /home/airflow/.local/bin/airflow dags trigger retail_medallion_pipeline --run-id $runId --conf $airflowConf
docker compose exec -T airflow /home/airflow/.local/bin/python /home/airflow/.local/bin/airflow dags list-runs retail_medallion_pipeline --no-backfill
```

Open the `run_pipeline` log in Airflow and search for `CDC MANIFEST`, `CHANGE WINDOW`, `BATCH NO CHANGES`, and `PIPELINE SUCCESS`. The manifest must report `update: 1`, and the only change window must be `2009-12-01..2009-12-01`.

## 3. Verify the changed row in PostgreSQL

```powershell
docker compose exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select invoice_no, stock_code, description, sales_date from silver.retail_transactions where invoice_no=''489434'' and stock_code=''85048'';"'
```

Expected description: `15CM CHRISTMAS GLASS BALL 20 LIGHTS CDC_TEST_UPDATE`.

## 4. Restore the source and run incremental again

Restore the same field with an exact, byte-safe replacement:

```powershell
$sourcePath = (Resolve-Path data/landing_csv/online_retail_II.csv).Path
$encoding = [System.Text.Encoding]::Latin1
$changedValue = '489434,85048,15CM CHRISTMAS GLASS BALL 20 LIGHTS CDC_TEST_UPDATE,12,2009-12-01 07:45:00,6.95,13085.0,United Kingdom'
$originalValue = '489434,85048,15CM CHRISTMAS GLASS BALL 20 LIGHTS,12,2009-12-01 07:45:00,6.95,13085.0,United Kingdom'
$content = [System.IO.File]::ReadAllText($sourcePath, $encoding)
if (([regex]::Matches($content, [regex]::Escape($changedValue))).Count -ne 1) { throw 'Expected exactly one changed source row' }
[System.IO.File]::WriteAllText($sourcePath, $content.Replace($changedValue, $originalValue), $encoding)

Get-FileHash -Algorithm SHA256 data/landing_csv/online_retail_II.csv
Get-FileHash -Algorithm SHA256 data/test_backups/online_retail_II.before_incremental_test.csv

$restoreRunId = 'portfolio_incremental_restore_' + (Get-Date -Format yyyyMMdd_HHmmss)
$airflowConf = "{`"mode`":`"incremental`"}"
docker compose exec -T airflow /home/airflow/.local/bin/python /home/airflow/.local/bin/airflow dags trigger retail_medallion_pipeline --run-id $restoreRunId --conf $airflowConf
```

Both hashes must be identical. After the restore run succeeds, repeat the PostgreSQL query and confirm the original description is present. Then remove the local backup:

```powershell
Remove-Item -LiteralPath data/test_backups/online_retail_II.before_incremental_test.csv
```

## 5. Portfolio checks before commit

```powershell
python -m py_compile airflow/dags/retail_medallion_pipeline.py spark_jobs/00_snapshot_cdc.py spark_jobs/01_temp_parquet_to_bronze.py spark_jobs/03_warehouse_to_parquet.py
docker compose config --quiet
docker compose exec -T airflow /home/airflow/.local/bin/python /home/airflow/.local/bin/airflow dags list-import-errors
git diff --check
git status --short
```

Commit only after the restore run and all checks succeed:

```powershell
git add .
git commit -m "feat: add optimized snapshot CDC incremental pipeline"
git push -u origin main
```
