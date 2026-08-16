#!/bin/bash

# ==============================================================================
# CMU OpenCloud Analytics Pipeline Orchestration
# 
# This script sequentially runs all batch processing phases developed for the 
# OpenCloud dataset. It cleans up output directories between runs to ensure 
# idempotency.
#
# Requirements:
# - Docker must be running
# - Hadoop cluster (namenode, resourcemanager) and Spark cluster must be up
# - Project JARs must be compiled and deployed
# ==============================================================================

set -e

echo "=========================================================="
echo " Starting CMU OpenCloud Analytics Pipeline"
echo "=========================================================="

# 1. Clean previous HDFS output directories
echo "[1/6] Cleaning previous HDFS output directories..."
docker exec namenode bash -c "hdfs dfs -rm -r -f /project/batch_results/*" || true
docker exec namenode bash -c "hdfs dfs -rm -r -f /project/processed/*" || true

# 2. Phase 5: Java MapReduce Job
echo "\n[2/6] Executing Phase 5: Java MapReduce (JobStatusCount)..."
docker exec resourcemanager hadoop jar /project/src/java/MapReduceProcessor.jar edu.project1.mapreduce.JobStatusCount /project/raw/cmu/2010-05/job.csv /project/batch_results/mr_output

# 3. Phase 6: Spark RDD Job
echo "\n[3/6] Executing Phase 6: Spark RDD (RDDJobParser)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.rdd.RDDJobParser \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 4. Phase 7: Spark DataFrame Job
echo "\n[4/6] Executing Phase 7: Spark DataFrame (CMUDataFrameProcessor)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.dataframe.CMUDataFrameProcessor \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 5. Phase 8: Spark Dataset Job
echo "\n[5/6] Executing Phase 8: Spark Dataset API (CMUDatasetProcessor)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.dataset.CMUDatasetProcessor \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

# 6. Phase 9: Spark SQL Job
echo "\n[6/6] Executing Phase 9: Spark SQL (CMUSQLProcessor)..."
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class edu.project1.sql.CMUSQLProcessor \
  --driver-memory 2g \
  --executor-memory 2g \
  /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar

echo "\n=========================================================="
echo " Pipeline Execution Complete! All phases succeeded."
echo "=========================================================="
