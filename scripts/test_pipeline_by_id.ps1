param(
    [datetime]$StartDate = [datetime]'2009-12-01',
    [datetime]$EndDate = [datetime]'2011-12-04',
    [int]$BatchDays = 50,
    [int]$BatchIdOffset = 0,
    [int]$SkipThroughJobId = 0,
    [string]$PostgresUser = 'retail_admin'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$runId = 'manual-test-20091201-20111204-20260901'
$results = [System.Collections.Generic.List[object]]::new()

$jobs = @(
    [pscustomobject]@{ Id=100; Priority=10; Group='bronze.retail'; Table='bronze.retail'; Kind='spark'; Args=@('01_temp_parquet_to_bronze.py') },
    [pscustomobject]@{ Id=200; Priority=10; Group='temp.retail_batch'; Table='temp.retail_batch'; Kind='spark-jdbc'; Args=@('02_bronze_to_temp_sql.py') },
    [pscustomobject]@{ Id=300; Priority=10; Group='silver.retail'; Table='silver.common'; Kind='dbt'; Args=@('run','int_retail_classified') },
    [pscustomobject]@{ Id=310; Priority=20; Group='silver.retail'; Table='silver.retail_rejects'; Kind='dbt'; Args=@('run','retail_rejects') },
    [pscustomobject]@{ Id=320; Priority=30; Group='silver.retail'; Table='silver.retail_duplicates'; Kind='dbt'; Args=@('run','retail_duplicates') },
    [pscustomobject]@{ Id=330; Priority=40; Group='silver.retail'; Table='silver.retail_transactions'; Kind='dbt'; Args=@('run','retail_transactions') },
    [pscustomobject]@{ Id=340; Priority=50; Group='silver.retail'; Table='silver.validation'; Kind='dbt'; Args=@('test','path:tests') },
    [pscustomobject]@{ Id=400; Priority=10; Group='gold.dimensions'; Table='gold.dim_date'; Kind='dbt'; Args=@('run','dim_date') },
    [pscustomobject]@{ Id=410; Priority=20; Group='gold.dimensions'; Table='gold.dim_product'; Kind='dbt'; Args=@('run','dim_product') },
    [pscustomobject]@{ Id=420; Priority=30; Group='gold.dimensions'; Table='gold.dim_country'; Kind='dbt'; Args=@('run','dim_country') },
    [pscustomobject]@{ Id=500; Priority=10; Group='gold.facts'; Table='gold.fact_sales'; Kind='dbt'; Args=@('run','fact_sales') },
    [pscustomobject]@{ Id=510; Priority=20; Group='gold.facts'; Table='gold.validation'; Kind='dbt'; Args=@('test','path:models/gold') },
    [pscustomobject]@{ Id=600; Priority=10; Group='export'; Table='silver_and_gold_parquet'; Kind='spark-jdbc'; Args=@('03_warehouse_to_parquet.py') }
) | Sort-Object Id, Priority

function Invoke-CheckedCommand {
    param([string[]]$Command)
    & $Command[0] $Command[1..($Command.Count - 1)]
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

function Save-TimingResult {
    param([pscustomobject]$Result)
    $sql = @"
CREATE TABLE IF NOT EXISTS audit.pipeline_test_timing (
    run_id text NOT NULL, batch_id integer NOT NULL, batch_start date NOT NULL,
    batch_end date NOT NULL, job_id integer NOT NULL, priority integer NOT NULL,
    table_group text NOT NULL, table_name text NOT NULL,
    started_at timestamp NOT NULL, ended_at timestamp NOT NULL,
    duration_seconds numeric NOT NULL, status text NOT NULL,
    PRIMARY KEY (run_id, batch_id, job_id)
);
INSERT INTO audit.pipeline_test_timing VALUES (
    '$runId', $($Result.batch_id), '$($Result.batch_start)', '$($Result.batch_end)',
    $($Result.job_id), $($Result.priority), '$($Result.table_group)', '$($Result.table_name)',
    '$($Result.started_at)', '$($Result.ended_at)', $($Result.duration_seconds), '$($Result.status)'
) ON CONFLICT (run_id, batch_id, job_id) DO UPDATE SET
    started_at=EXCLUDED.started_at, ended_at=EXCLUDED.ended_at,
    duration_seconds=EXCLUDED.duration_seconds, status=EXCLUDED.status;
"@
    Invoke-CheckedCommand -Command @(
        'docker', 'compose', 'exec', '-T', 'postgres',
        'psql', '-v', 'ON_ERROR_STOP=1', '-U', $PostgresUser, '-d', 'warehouse', '-c', $sql
    )
}

$batches = @()
$cursor = $StartDate
while ($cursor -le $EndDate) {
    $batchEnd = $cursor.AddDays($BatchDays - 1)
    if ($batchEnd -gt $EndDate) { $batchEnd = $EndDate }
    $batches += ,@($cursor, $batchEnd)
    $cursor = $batchEnd.AddDays(1)
}

try {
    for ($batchIndex = 0; $batchIndex -lt $batches.Count; $batchIndex++) {
        $start = $batches[$batchIndex][0].ToString('yyyy-MM-dd')
        $end = $batches[$batchIndex][1].ToString('yyyy-MM-dd')

        foreach ($job in $jobs) {
            if ($batchIndex -eq 0 -and $job.Id -le $SkipThroughJobId) {
                continue
            }
            $startedAt = Get-Date
            $status = 'SUCCESS'
            try {
                if ($job.Kind -eq 'dbt') {
                    $vars = "{start_date: $start, end_date: $end}"
                    $command = @(
                        'docker', 'compose', 'run', '--rm', 'dbt',
                        'dbt', $job.Args[0],
                        '--project-dir', '/usr/app/dbt/retail_warehouse',
                        '--profiles-dir', '/usr/app/dbt/retail_warehouse',
                        '--select', $job.Args[1], '--vars', $vars
                    )
                } else {
                    $command = @('docker', 'compose', 'exec', '-T', 'airflow', '/opt/spark/bin/spark-submit', '--master', 'spark://spark-master:7077')
                    if ($job.Kind -eq 'spark-jdbc') {
                        $jar = '/opt/pipeline/spark_jobs/lib/postgresql-42.7.7.jar'
                        $command += @('--jars', $jar, '--driver-class-path', $jar)
                    }
                    $command += @("/opt/pipeline/spark_jobs/$($job.Args[0])", $start, $end)
                }
                Invoke-CheckedCommand -Command $command
            } catch {
                $status = 'FAILED'
                throw
            } finally {
                $endedAt = Get-Date
                $result = [pscustomobject]@{
                    batch_id = $BatchIdOffset + $batchIndex + 1
                    batch_start = $start
                    batch_end = $end
                    job_id = $job.Id
                    priority = $job.Priority
                    table_group = $job.Group
                    table_name = $job.Table
                    started_at = $startedAt.ToString('yyyy-MM-dd HH:mm:ss')
                    ended_at = $endedAt.ToString('yyyy-MM-dd HH:mm:ss')
                    duration_seconds = [math]::Round(($endedAt - $startedAt).TotalSeconds, 2)
                    status = $status
                }
                $results.Add($result)
                Save-TimingResult -Result $result
                Write-Output "TIMING batch=$($result.batch_id) job=$($result.job_id) status=$($result.status) seconds=$($result.duration_seconds)"
            }
        }
    }
} finally {}
