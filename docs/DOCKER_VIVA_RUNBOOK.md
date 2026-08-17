# DOCKER VIVA RUNBOOK
## Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Course: 23AID302 — Big Data Analytics
## Last Updated: 2026-08-17 (Post-Remediation Verified)

> **AUDIT STATUS: FULLY VERIFIED ON LIVE 5-CONTAINER HADOOP/SPARK CLUSTER.**
> All commands in this runbook have been tested and verified against the running environment.

---

## PART 1 — CLUSTER TOPOLOGY & ARCHITECTURE

```
┌────────────────────────────────────────────────────────────────────────┐
│                      Docker Network: bigdata_net                       │
│                            (bridge driver)                             │
│                                                                        │
│  ┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐ │
│  │    namenode     │     │    datanode1     │     │    datanode2     │ │
│  │  Hadoop 3.2.1   │     │   Hadoop 3.2.1   │     │   Hadoop 3.2.1   │ │
│  │  Port: 9870     │     │   (internal)     │     │   (internal)     │ │
│  │  Port: 9000     │     │                  │     │                  │ │
│  └────────┬────────┘     └────────┬─────────┘     └────────┬─────────┘ │
│           │                       │                        │           │
│           └───────────────────────┴────────────────────────┘           │
│                                   │                                    │
│                       HDFS Default FS: port 9000                       │
│                       Replication Factor: 2                            │
│                                                                        │
│  ┌───────────────────────────────┐   ┌──────────────────────────────┐  │
│  │         spark-master          │   │         spark-worker         │  │
│  │          Spark 3.2.1          │   │          Spark 3.2.1         │  │
│  │  UI: 8080 | Master: 7077      │   │  UI: 8081 | 4 Cores | 4 GB   │  │
│  └───────────────────────────────┘   └──────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### Verified Container Inventory

| Container Name | Image | Component | Host Ports | Attached Volume |
|---|---|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | HDFS NameNode | `9870`, `9000` | `hadoop_namenode`, `D:/BigData/Project_1:/host-project` |
| `datanode1` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | HDFS DataNode 1 | none | `hadoop_datanode1`, `D:/BigData/Project_1:/host-project` |
| `datanode2` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | HDFS DataNode 2 | none | `hadoop_datanode2`, `D:/BigData/Project_1:/host-project` |
| `spark-master` | `bde2020/spark-master:3.2.1-hadoop3.2` | Spark Master | `8080`, `7077` | `D:/BigData/Project_1:/host-project` |
| `spark-worker` | `bde2020/spark-worker:3.2.1-hadoop3.2` | Spark Worker (4 cores, 4GB) | `8081` | `D:/BigData/Project_1:/host-project` |

### Architectural Facts for Viva Defense
- **NameNodes:** 1 (`namenode` container) — holds file metadata and block maps.
- **DataNodes:** 2 (`datanode1`, `datanode2`) — hold replicated physical 128 MB blocks.
- **HDFS Replication Factor:** **2** (`HDFS_CONF_dfs_replication=2`). Every block has 2 copies.
- **YARN / ResourceManager:** None. MapReduce runs in **LOCAL** mode inside `namenode`.
- **Spark Cluster:** Standalone Spark Master + 1 Worker (4 CPU cores, 4 GB RAM).
- **Host Storage:** Docker volumes on E: drive (`E:\Docker\DockerDesktop\`).

---

## PART 2 — CLUSTER LIFECYCLE & STARTUP

### Step 1: Start Docker Desktop & Launch Cluster
```powershell
cd D:\BigData\Project_1\docker
docker-compose up -d
```
*Wait ~45 seconds for NameNode healthcheck.*

### Step 2: Verify Container Status
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
*Expected: 5 containers with status `Up ... (healthy)` or `Up ...`.*

---

## PART 3 — ESSENTIAL VIVA DEMONSTRATION COMMANDS

### 1. Verify HDFS Health & 2 Live DataNodes
```powershell
docker exec namenode hdfs dfsadmin -report
```
- **Purpose:** Proves that the NameNode recognizes 2 healthy DataNodes and shows storage usage.
- **Expected Output:** `Live datanodes (2): datanode1, datanode2` with 80+ GB DFS used.
- **Viva Screenshot:** `SS-01`

### 2. Verify Raw Big Data Volumes in HDFS
```powershell
docker exec namenode hdfs dfs -du -s -h /project/raw/*
```
- **Purpose:** Demonstrates Volume (22 GB CMU structured traces + 16.1 GB HDFS_v2 unstructured logs).
- **Expected Output:**
  - `22.0 G  /project/raw/cmu`
  - `16.1 G  /project/raw/hdfs_v2`
- **Viva Screenshots:** `SS-04`, `SS-05`

### 3. Verify Batch Pipeline Orchestration (Fixed & Verified)
```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1
```
- **Purpose:** Sequentially runs MapReduce, RDD, DataFrame, Dataset, and Spark SQL jobs with cleanup.
- **Expected Output:** `Pipeline Execution Complete! All phases succeeded.`
- **Viva Screenshot:** `SS-22`

### 4. Verify Java MapReduce Baseline Output
```powershell
docker exec namenode hdfs dfs -cat /project/mapreduce_output/job_status_counts/part-r-00000
```
- **Purpose:** Shows raw class aggregation produced by Java MapReduce in 3.6 seconds.
- **Expected Output:**
  ```
  FAILED     4534
  KILLED     1666
  SUCCESS    49026
  UNKNOWN_6  26
  ```
- **Viva Screenshot:** `SS-11`

### 5. Verify Processed Parquet Data in HDFS
```powershell
docker exec namenode hdfs dfs -ls /project/processed/cmu_jobs
```
- **Purpose:** Shows that Spark DataFrame partitioned and saved clean data back to HDFS by binary label.
- **Expected Output:** `label=0` (SUCCESS) and `label=1` (FAILED) partition directories with Snappy Parquet files.
- **Viva Screenshot:** `SS-16`

### 6. Run Machine Learning & Evaluate Models
**Logistic Regression (Phase 12):**
```powershell
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMULogisticRegression /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
**Random Forest (Phase 13):**
```powershell
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMURandomForest /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Metrics Computed:** Accuracy, Weighted Precision, Recall, F1, ROC-AUC, Confusion Matrix, Minority `FAILED` class recall.
- **Results:**
  - **Random Forest:** Accuracy: `92.28%`, ROC-AUC: `0.8947`, Minority Recall: `19.30%` (21x better than LR), Precision: `61.73%`.
  - **Logistic Regression:** Accuracy: `91.53%`, ROC-AUC: `0.6871`, Minority Recall: `0.90%`.
- **Viva Screenshots:** `SS-23`, `SS-24`

### 7. Verify Persisted ML Models in HDFS
```powershell
docker exec namenode hdfs dfs -ls -R /project/models
```
- **Purpose:** Confirms trained MLlib models are stored in HDFS and can be reloaded independently.
- **Expected Output:**
  - `/project/models/logistic_regression/data` and `metadata`
  - `/project/models/random_forest/data`, `metadata`, and `treesMetadata`

### 8. Run K-Means Anomaly Detection (Phase 14)
```powershell
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUKMeansAnomaly /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Groups 294,649 five-minute node log windows into 5 clusters and flags metadata storms on `namenode` (>100,000 events/5m).
- **Viva Screenshot:** `SS-26`

### 9. Run Structured Streaming Historical Replay (Phase 15)
```powershell
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.streaming.CMUHistoricalStreaming /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Replays historical HDFS_v2 logs file-by-file into 5-minute tumbling windows and flags real-time anomalies.
- **Viva Evidence:** `SS-28` (`evidence/phase15/SS-28_historical_streaming_output.txt`)

### 10. Run Concept Drift Detection (Phase 16)
```powershell
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUDriftDetection /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Statistical Z-score check on `totalTasks`, `mapReduceRatio`, and `queueWaitMs` (All Z < 0.1, STABLE).
- **Viva Screenshot:** `SS-29`

### 11. Run Operational Alert Monitor (Phase 17)
```powershell
python src/python/alert_monitor.py
```
- **Purpose:** Formats terminal alerts for K-Means anomalies and concept drift monitoring.
- **Viva Screenshot:** `SS-30`

### 12. Launch Streamlit Analytics Dashboard (Phase 18)
```powershell
streamlit run src/python/dashboard.py
```
- **URL:** `http://localhost:8501`
- **Purpose:** Real-time web UI showing Model Health (Accuracy, AUC), Metadata Storm Alerts, and Drift Metrics.
- **Viva Screenshot:** `SS-31`

### 13. Run End-to-End Performance Benchmarks (Phase 19)
```powershell
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.benchmarking.CMUPerformanceBenchmarker /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Evaluates API overhead (RDD vs DF vs DS), Partitioning (2 vs 200 vs 10), and Caching (None vs Memory vs Disk).
- **Viva Screenshot:** `SS-32`

---

## PART 4 — WEB UI ACCESS FOR VIVA

| Interface | Host URL | What to Show |
|---|---|---|
| **HDFS NameNode UI** | `http://localhost:9870` | 2 live DataNodes, total capacity, Browse Directory `/project/raw` |
| **Spark Master UI** | `http://localhost:8080` | Registered Worker (4 cores, 4 GB RAM), completed Spark applications |
| **Spark Worker UI** | `http://localhost:8081` | Running/Finished executors, core allocation |
| **Streamlit Dashboard** | `http://localhost:8501` | Interactive analytics, RF metrics, metadata storm warnings |

---

## PART 5 — CLEAN SHUTDOWN PROCEDURE

```powershell
cd D:\BigData\Project_1\docker
docker-compose down
```
*Stops and removes all 5 containers while preserving volume data on E: drive.*
