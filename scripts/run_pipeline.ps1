# ==============================================================================
# CMU OpenCloud Analytics Pipeline Orchestration (PowerShell Version)
#
# Sequentially runs all batch processing phases for the CMU OpenCloud dataset.
# Cleans HDFS output directories between runs for idempotency.
# ==============================================================================

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Starting CMU OpenCloud Analytics Pipeline" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 0. Sanity-check containers
Write-Host ""
Write-Host "[0/7] Verifying Docker containers are up..." -ForegroundColor Yellow
$containers = @("namenode", "datanode1", "datanode2", "spark-master", "spark-worker")
foreach ($c in $containers) {
    $status = (docker inspect --format='{{.State.Status}}' $c 2>$null)
    if ($status -ne "running") {
        Write-Host "[ERROR] Container '$c' is not running (status: $status). Aborting." -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] $c is running" -ForegroundColor Green
}

# 1. Clean previous HDFS output directories
Write-Host ""
Write-Host "[1/7] Cleaning previous HDFS output directories..." -ForegroundColor Yellow
docker exec namenode bash -c 'hdfs dfs -rm -r -f /project/batch_results/* 2>/dev/null; exit 0'
docker exec namenode bash -c 'hdfs dfs -rm -r -f /project/mapreduce_output/* 2>/dev/null; exit 0'
docker exec namenode bash -c 'hdfs dfs -rm -r -f /project/processed/* 2>/dev/null; exit 0'
Write-Host "  [OK] Output directories cleared" -ForegroundColor Green

# 2. Phase 5: Java MapReduce Job
Write-Host ""
Write-Host "[2/7] Executing Phase 5: Java MapReduce (JobStatusDriver)..." -ForegroundColor Yellow
docker exec namenode bash -c 'time hadoop jar /opt/hadoop-apps/job-status-mr.jar edu.project1.mapreduce.JobStatusDriver'
Write-Host "  [OK] MapReduce output verification:" -ForegroundColor Green
docker exec namenode bash -c 'hdfs dfs -cat /project/mapreduce_output/job_status_counts/part-r-00000'

# 3. Phase 6: Spark RDD Job
Write-Host ""
Write-Host "[3/7] Executing Phase 6: Spark RDD (RDDJobParser)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.rdd.RDDJobParser --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 4. Phase 7: Spark DataFrame Job
Write-Host ""
Write-Host "[4/7] Executing Phase 7: Spark DataFrame (CMUDataFrameProcessor)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.dataframe.CMUDataFrameProcessor --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 5. Phase 8: Spark Dataset Job
Write-Host ""
Write-Host "[5/7] Executing Phase 8: Spark Dataset API (CMUDatasetProcessor)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.dataset.CMUDatasetProcessor --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 6. Phase 9: Spark SQL Job
Write-Host ""
Write-Host "[6/7] Executing Phase 9: Spark SQL (CMUSQLProcessor)..." -ForegroundColor Yellow
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.sql.CMUSQLProcessor --driver-memory 2g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 7. Final HDFS Verification
Write-Host ""
Write-Host "[7/7] Verifying HDFS outputs..." -ForegroundColor Yellow
docker exec namenode bash -c 'hdfs dfs -du -s -h /project/batch_results/*'
docker exec namenode bash -c 'hdfs dfs -du -s -h /project/processed/*'
docker exec namenode bash -c 'hdfs dfs -du -s -h /project/mapreduce_output/*'

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Pipeline Execution Complete! All phases succeeded." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
