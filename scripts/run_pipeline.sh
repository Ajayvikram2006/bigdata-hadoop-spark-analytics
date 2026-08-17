#!/bin/bash

# ==============================================================================
# CMU OpenCloud Analytics Pipeline Orchestration
#
# Sequentially runs all batch processing phases for the CMU OpenCloud dataset.
# Cleans HDFS output directories between runs for idempotency.
#
# Requirements:
#   - Docker Desktop must be running
#   - All 5 containers must be up: namenode, datanode1, datanode2, spark-master, spark-worker
#   - Hadoop namenode must be in safe mode exit (healthy)
#   - Spark JAR deployed at: /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
#   - MapReduce JAR deployed at: /opt/hadoop-apps/job-status-mr.jar (inside namenode)
#
# Cluster architecture:
#   HDFS: 1 NameNode + 2 DataNodes (no YARN/ResourceManager)
#   Spark: 1 Master + 1 Worker (standalone cluster mode)
#   MapReduce: LOCAL mode inside namenode container
#
# Usage (from Windows PowerShell, Git Bash, or WSL):
#   bash scripts/run_pipeline.sh
#
# Updated: 2026-08-17 (Remediation — fixed container, JAR, and class names)
# ==============================================================================

set -e

echo "=========================================================="
echo "  Starting CMU OpenCloud Analytics Pipeline"
echo "=========================================================="

# 0. Sanity-check: confirm required containers are running
echo ""
echo "[0/7] Verifying Docker containers are up..."
for CONTAINER in namenode datanode1 datanode2 spark-master spark-worker; do
  STATUS=$(docker inspect --format='{{.State.Status}}' "$CONTAINER" 2>/dev/null || echo "not_found")
  if [ "$STATUS" != "running" ]; then
    echo "[ERROR] Container '$CONTAINER' is not running (status: $STATUS). Aborting."
    echo "  Run: cd docker && docker-compose up -d"
    exit 1
  fi
  echo "  ✓ $CONTAINER is running"
done

# 1. Clean previous HDFS output directories (idempotent)
echo ""
echo "[1/7] Cleaning previous HDFS output directories..."
docker exec namenode bash -c "hdfs dfs -rm -r -f /project/batch_results/* 2>/dev/null || true"
docker exec namenode bash -c "hdfs dfs -rm -r -f /project/mapreduce_output/* 2>/dev/null || true"
docker exec namenode bash -c "hdfs dfs -rm -r -f /project/processed/* 2>/dev/null || true"
docker exec namenode bash -c "hdfs dfs -rm -r -f /project/models/* 2>/dev/null || true"
echo "  ✓ Output directories cleared"

# 2. Phase 5: Java MapReduce Job (local mode inside namenode container)
echo ""
echo "[2/7] Executing Phase 5: Java MapReduce (JobStatusDriver)..."
echo "  Container : namenode"
echo "  JAR       : /opt/hadoop-apps/job-status-mr.jar"
echo "  Class     : edu.project1.mapreduce.JobStatusDriver"
echo "  Mode      : LOCAL (mapreduce.framework.name=local)"
docker exec namenode bash -c "
  time hadoop jar /opt/hadoop-apps/job-status-mr.jar \
    edu.project1.mapreduce.JobStatusDriver \
  2>&1
"
echo "  ✓ MapReduce complete. Verifying output..."
docker exec namenode bash -c "hdfs dfs -cat /project/mapreduce_output/job_status_counts/part-r-00000"

# 3. Phase 6: Spark RDD Job
echo ""
echo "[3/7] Executing Phase 6: Spark RDD (RDDJobParser)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.rdd.RDDJobParser \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 4. Phase 7: Spark DataFrame Job (also writes processed Parquet)
echo ""
echo "[4/7] Executing Phase 7: Spark DataFrame (CMUDataFrameProcessor)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.dataframe.CMUDataFrameProcessor \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 5. Phase 8: Spark Dataset Job
echo ""
echo "[5/7] Executing Phase 8: Spark Dataset API (CMUDatasetProcessor)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.dataset.CMUDatasetProcessor \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 6. Phase 9: Spark SQL Job (produces ML input at /project/batch_results/sql_output/)
echo ""
echo "[6/7] Executing Phase 9: Spark SQL (CMUSQLProcessor)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.sql.CMUSQLProcessor \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 7. Final HDFS verification
echo ""
echo "[7/7] Verifying HDFS outputs..."
docker exec namenode bash -c "hdfs dfs -du -s -h /project/batch_results/*"

echo ""
echo "=========================================================="
echo "  Pipeline Execution Complete! All phases succeeded."
echo ""
echo "  Next steps:"
echo "  - Run ML jobs:  spark-submit CMULogisticRegression / CMURandomForest"
echo "  - Run anomaly:  spark-submit CMUKMeansAnomaly"
echo "  - Run drift:    spark-submit CMUDriftDetection"
echo "  - Run benchmark: spark-submit CMUPerformanceBenchmarker"
echo "=========================================================="
