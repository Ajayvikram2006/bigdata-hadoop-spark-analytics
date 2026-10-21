# ==============================================================================
# CMU OpenCloud Analytics Pipeline Orchestration (PowerShell Version)
#
# Sequentially runs all batch processing phases for the CMU OpenCloud dataset.
# Cleans only RDD text output directories between runs for idempotency.
# Performs strict exit-code checking after every step and auto-stages JARs.
# ==============================================================================

$ErrorActionPreference = "Stop"

function Assert-LastExitCode {
    param([string]$StepName)
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Step '$StepName' failed with exit code $LASTEXITCODE. Aborting pipeline." -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Starting CMU OpenCloud Analytics Pipeline" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 0. Sanity-check containers & deployed JARs
Write-Host ""
Write-Host "[0/7] Verifying Docker containers and deployed JARs..." -ForegroundColor Yellow
$containers = @("namenode", "datanode1", "datanode2", "spark-master", "spark-worker")
foreach ($c in $containers) {
    $status = (docker inspect --format='{{.State.Status}}' $c 2>$null)
    if ($status -ne "running") {
        Write-Host "[ERROR] Container '$c' is not running (status: $status). Aborting." -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] $c is running" -ForegroundColor Green
}

docker exec spark-master bash -c 'mkdir -p /opt/spark-apps && if [ ! -f /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar ]; then cp /host-project/src/scala/target/scala-2.12/bigdata-analytics-assembly-1.0.0.jar /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar; fi && test -f /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar'
Assert-LastExitCode "Stage Spark assembly JAR in spark-master"

docker exec namenode bash -c 'if [ ! -f /opt/hadoop-apps/job-status-mr.jar ]; then bash /host-project/scripts/build_mr.sh; fi && test -f /opt/hadoop-apps/job-status-mr.jar'
Assert-LastExitCode "Verify MapReduce JAR in namenode"

# 1. Clean only the specific RDD text output directories (saveAsTextFile requires non-existent target path)
# SAFETY REMEDIATION (2026-10-21): Broad wildcard deletes (/project/processed/*, /project/batch_results/*, /project/models/*)
# remain disabled so /project/processed/hdfs_logs and /project/models/* are never wiped.
Write-Host ""
Write-Host "[1/7] Cleaning only RDD text output directories (preserving /project/processed/hdfs_logs and /project/models/*)..." -ForegroundColor Yellow
docker exec namenode bash -c 'hdfs dfs -rm -r -f /project/batch_results/rdd_status_counts /project/batch_results/rdd_queue_stats'
Assert-LastExitCode "Targeted RDD output cleanup"
Write-Host "  [OK] Targeted RDD output cleanup complete (all other HDFS directories preserved)" -ForegroundColor Green

# 2. Phase 5: Java MapReduce Job
Write-Host ""
Write-Host "[2/7] Executing Phase 5: Java MapReduce (JobStatusDriver)..." -ForegroundColor Yellow
docker exec namenode bash -c 'time hadoop jar /opt/hadoop-apps/job-status-mr.jar edu.project1.mapreduce.JobStatusDriver'
Assert-LastExitCode "Phase 5: Java MapReduce (JobStatusDriver)"
Write-Host "  [OK] MapReduce output verification:" -ForegroundColor Green
docker exec namenode bash -c 'hdfs dfs -cat /project/mapreduce_output/job_status_counts/part-r-00000'
Assert-LastExitCode "Verify MapReduce output"

# 3. Phase 6: Spark RDD Job
Write-Host ""
Write-Host "[3/7] Executing Phase 6: Spark RDD (RDDJobParser)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.dfs.replication=2 --class edu.project1.rdd.RDDJobParser --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
Assert-LastExitCode "Phase 6: Spark RDD (RDDJobParser)"

# 4. Phase 7: Spark DataFrame Job
Write-Host ""
Write-Host "[4/7] Executing Phase 7: Spark DataFrame (CMUDataFrameProcessor)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.dfs.replication=2 --class edu.project1.dataframe.CMUDataFrameProcessor --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
Assert-LastExitCode "Phase 7: Spark DataFrame (CMUDataFrameProcessor)"

# 5. Phase 8: Spark Dataset Job
Write-Host ""
Write-Host "[5/7] Executing Phase 8: Spark Dataset API (CMUDatasetProcessor)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.dfs.replication=2 --class edu.project1.dataset.CMUDatasetProcessor --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
Assert-LastExitCode "Phase 8: Spark Dataset API (CMUDatasetProcessor)"

# 6. Phase 9: Spark SQL Job
Write-Host ""
Write-Host "[6/7] Executing Phase 9: Spark SQL (CMUSQLProcessor)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.dfs.replication=2 --class edu.project1.sql.CMUSQLProcessor --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
Assert-LastExitCode "Phase 9: Spark SQL (CMUSQLProcessor)"

# 7. Final HDFS Verification
Write-Host ""
Write-Host "[7/7] Verifying HDFS outputs..." -ForegroundColor Yellow
docker exec namenode bash -c 'hdfs dfs -du -s -h /project/batch_results/*'
Assert-LastExitCode "Verify /project/batch_results/*"
docker exec namenode bash -c 'hdfs dfs -du -s -h /project/processed/*'
Assert-LastExitCode "Verify /project/processed/*"
docker exec namenode bash -c 'hdfs dfs -du -s -h /project/mapreduce_output/*'
Assert-LastExitCode "Verify /project/mapreduce_output/*"

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Pipeline Execution Complete! All phases succeeded." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
