# DOCKER VIVA RUNBOOK
## Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Course: 23AID302 — Big Data Analytics
## Last Updated: 2026-08-17 (Post-Remediation Master Edition)

> **AUDIT STATUS: FULLY VERIFIED ON ACTIVE 5-CONTAINER DISTRIBUTED CLUSTER.**
> This handbook is your master operational guide for viva defense, terminal demonstrations, and cluster troubleshooting.

---

# PART 1 — DOCKER CLUSTER TOPOLOGY & ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       Docker Host (Windows 11)                                          │
│                                  Docker Bridge Network: bigdata_net                                     │
│                                                                                                         │
│  ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐                │
│  │        namenode         │   │        datanode1        │   │        datanode2        │                │
│  │  Hadoop 3.2.1 (Master)  │   │  Hadoop 3.2.1 (Worker)  │   │  Hadoop 3.2.1 (Worker)  │                │
│  │  Port 9870: Web UI      │   │  Port 9864: Internal    │   │  Port 9864: Internal    │                │
│  │  Port 9000: HDFS IPC    │   │  Block Replica 1        │   │  Block Replica 2        │                │
│  │  MapReduce: LOCAL Mode  │   │                         │   │                         │                │
│  └────────────┬────────────┘   └────────────┬────────────┘   └────────────┬────────────┘                │
│               │                             │                             │                             │
│               └─────────────────────────────┴─────────────────────────────┘                             │
│                                             │                                                           │
│                                             ▼                                                           │
│                             HDFS Distributed Storage Layer                                              │
│                            hdfs://namenode:9000/project/                                                │
│                            Replication Factor = 2 | Block Size = 128 MB                                 │
│                                             │                                                           │
│               ┌─────────────────────────────┴─────────────────────────────┐                             │
│               │                                                           │                             │
│  ┌────────────┴────────────┐                             ┌────────────────┴────────────┐                │
│  │      spark-master       │                             │        spark-worker         │                │
│  │     Spark 3.2.1 Master  │◄───────────────────────────►│     Spark 3.2.1 Worker      │                │
│  │  Port 8080: Web UI      │     Netty RPC Protocol      │  Port 8081: Web UI          │                │
│  │  Port 7077: Master IPC  │                             │  4 CPU Cores | 4096 MB RAM  │                │
│  └─────────────────────────┘                             └─────────────────────────────┘                │
│                                                                                                         │
│  Host Volume Storage:                                                                                   │
│    - Host Workspace: D:\BigData\Project_1  ──►  Mounted at /host-project in all containers               │
│    - WSL2 Virtual Disk: E:\Docker\DockerDesktop\DockerDesktopWSL\data\ext4.vhdx                         │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Verified Container Inventory Table

| # | Container Name | Image Tag | Core Role | Host Ports | Attached Volume | Memory / Cores |
|---|---|---|---|---|---|---|
| 1 | `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | HDFS NameNode & Local MapReduce | `9870`, `9000` | `hadoop_namenode`, `D:/BigData/Project_1:/host-project` | 2 GB / 2 Cores |
| 2 | `datanode1` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | HDFS DataNode 1 (Block Store) | none | `hadoop_datanode1`, `D:/BigData/Project_1:/host-project` | 2 GB / 2 Cores |
| 3 | `datanode2` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | HDFS DataNode 2 (Block Store) | none | `hadoop_datanode2`, `D:/BigData/Project_1:/host-project` | 2 GB / 2 Cores |
| 4 | `spark-master` | `bde2020/spark-master:3.2.1-hadoop3.2` | Spark Standalone Master | `8080`, `7077` | `D:/BigData/Project_1:/host-project` | 2 GB / 2 Cores |
| 5 | `spark-worker` | `bde2020/spark-worker:3.2.1-hadoop3.2` | Spark Standalone Worker Executor | `8081` | `D:/BigData/Project_1:/host-project` | 4 GB / 4 Cores |

### Key Architectural Facts for Viva
- **Total Containers:** Exactly **5 containers**.
- **NameNodes:** Exactly **1** (`namenode`).
- **DataNodes:** Exactly **2** (`datanode1`, `datanode2`).
- **Replication Factor:** **2** (`HDFS_CONF_dfs_replication=2`). Every block has 2 physical copies.
- **YARN / ResourceManager:** **None.** MapReduce runs in **LOCAL mode** inside `namenode`. Spark uses its own Standalone Cluster Manager.

---

# PART 2 — CLUSTER STARTUP PROCEDURE

### Step 0: Start Docker Desktop
1. Launch **Docker Desktop** on Windows.
2. Ensure the system tray icon turns green ("Docker Desktop is running").

### Step 1: Navigate to Project Docker Directory
```powershell
cd D:\BigData\Project_1\docker
```

### Step 2: Bring Up All 5 Containers
```powershell
docker-compose up -d
```
- **What it does:** Starts all 5 containers in detached background mode on the `bigdata_net` bridge network.
- **Expected Output:**
  ```
  [+] Running 5/5
   ✔ Container namenode       Started
   ✔ Container datanode1      Started
   ✔ Container datanode2      Started
   ✔ Container spark-master   Started
   ✔ Container spark-worker   Started
  ```

### Step 3: Verify All Containers Are Up & Healthy
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
- **What to check:** All 5 containers show `Up` and DataNodes/NameNode show `(healthy)` (takes ~45 seconds).

---

# PART 3 — HDFS HEALTH & DATA VERIFICATION

### Command 1: Verify 2 Live DataNodes in HDFS
```powershell
docker exec namenode hdfs dfsadmin -report
```
- **Purpose:** Verifies that both DataNodes are registered and reports live cluster capacity.
- **Expected Output:**
  ```
  Configured Capacity: 2.16 TB
  DFS Used: 86.01 GB (4.41%)
  DFS Remaining: 1.86 TB
  Live datanodes (2):
    Name: 172.18.0.5:9866 (datanode1) ... Status : Normal
    Name: 172.18.0.6:9866 (datanode2) ... Status : Normal
  ```
- **Viva Screenshot:** `SS-01`
- **Viva Explanation:** "This proves that our Hadoop HDFS cluster consists of 2 live, healthy DataNodes storing 86 GB of raw and replicated Big Data."

### Command 2: Verify Raw Dataset Volumes in HDFS
```powershell
docker exec namenode hdfs dfs -du -s -h /project/raw/*
```
- **Purpose:** Proves Big Data Volume (22 GB CMU + 16.1 GB HDFS_v2 = 38.1 GB total).
- **Expected Output:**
  ```
  22.0 G   44.1 G   /project/raw/cmu
  16.1 G   32.1 G   /project/raw/hdfs_v2
  ```
  *(Second number represents physical storage including replication factor 2).*
- **Viva Screenshots:** `SS-04`, `SS-05`

### Command 3: Inspect Raw Schema of CMU `job.csv`
```powershell
docker exec namenode bash -c "hdfs dfs -cat /project/raw/cmu/2010-05/job.csv | head -n 5"
```
- **Purpose:** Demonstrates raw structured CSV schema headers and data rows.
- **Expected Output:**
  ```csv
  jtid,jobid,submitTime,launchTime,finishTime,status,numMaps,numReduces,finMaps,finReduces,failMaps,failReduces
  1275364800000,1,1275364802000,1275364805000,1275364835000,0,10,2,10,2,0,0
  ```
- **Viva Screenshot:** `SS-07`

### Command 4: Inspect Raw Target Status Distribution
```powershell
docker exec namenode bash -c "hdfs dfs -cat /project/raw/cmu/*/job.csv | cut -d',' -f6 | sort | uniq -c"
```
- **Purpose:** Shows raw status counts across all 20 monthly folders.
- **Expected Output:** `49026 (0 = SUCCESS)`, `4534 (1 = FAILED)`, `1666 (2 = KILLED)`, `26 (UNKNOWN_6)`.
- **Viva Screenshot:** `SS-08`

---

# PART 4 — PHASE 5: JAVA MAPREDUCE BASELINE

### Step 1: Compile MapReduce Code (if rebuilding)
```powershell
docker exec namenode bash -c "javac -classpath \$(hadoop classpath) -d /tmp /host-project/src/mapreduce/*.java && jar -cvf /opt/hadoop-apps/job-status-mr.jar -C /tmp/ ."
```
- **Viva Screenshot:** `SS-09`

### Step 2: Execute MapReduce Job
```powershell
docker exec namenode bash -c "time hadoop jar /opt/hadoop-apps/job-status-mr.jar edu.project1.mapreduce.JobStatusDriver"
```
- **Purpose:** Aggregates job termination statuses across all 20 monthly CSV files in HDFS.
- **Expected Output:** Job completes in **~3.6 seconds** processing 55,272 map input records.
- **Viva Screenshot:** `SS-10`

### Step 3: View MapReduce Output File in HDFS
```powershell
docker exec namenode hdfs dfs -cat /project/mapreduce_output/job_status_counts/part-r-00000
```
- **Expected Output:**
  ```
  FAILED     4534
  KILLED     1666
  SUCCESS    49026
  UNKNOWN_6  26
  ```
- **Viva Screenshot:** `SS-11`
- **Viva Explanation:** "The Java MapReduce Mapper reads each raw line, filters headers, and emits `(StatusLabel, 1)`. The Reducer sums the IntWritables. This established our exact ground-truth class distribution."

---

# PART 5 — PHASE 6: SPARK RDD PROCESSING

### Execute Spark RDD Job
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.rdd.RDDJobParser --driver-memory 1g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Demonstrates low-level distributed RDD transformations (`textFile`, `filter`, `map`, `reduceByKey`, `sortBy`).
- **Expected Output:** Parses 55,252 records, drops invalid entries, and writes status counts and queue wait times to HDFS.
- **Viva Screenshot:** `SS-13`
- **Viva Explanation:** "RDD provides direct low-level control over distributed collections without Catalyst optimization. We used `.cache()` to avoid recomputing the raw text parse when deriving two separate metrics."

---

# PART 6 — PHASE 7: SPARK DATAFRAME PROCESSING

### Execute Spark DataFrame Job
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.dataframe.CMUDataFrameProcessor --driver-memory 1g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Performs relational LEFT OUTER JOINs between `job.csv`, `task.csv`, and `attempt.csv`, drops 5 leakage columns, and saves clean Parquet partitions.
- **Expected Output:** Writes partitioned Parquet files to `/project/processed/cmu_jobs/`.
- **Viva Screenshots:** `SS-14` (Spark UI DAG), `SS-16` (HDFS Parquet listing)

---

# PART 7 — PHASE 8: SPARK DATASET (TYPED API)

### Execute Spark Dataset Job
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.dataset.CMUDatasetProcessor --driver-memory 1g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Demonstrates compile-time type safety using Scala case classes (`JobRecord`, `TaskAggRecord`, `JoinedJobRecord`) and typed `.joinWith()` transformations.
- **Expected Output:** Successfully processes 53,559 clean records.
- **Viva Screenshot:** `SS-18`

---

# PART 8 — PHASE 9: SPARK SQL PROCESSING

### Execute Spark SQL Job
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.sql.CMUSQLProcessor --driver-memory 1g --executor-memory 2g /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Executes complex SQL Common Table Expressions (CTEs) to join tables, engineer features, and print the Catalyst physical execution plan (`explain`).
- **Expected Output:** Generates `/project/batch_results/sql_output` (53,559 records) which serves as the direct input for ML training.
- **Viva Screenshot:** `SS-20`

---

# PART 9 — PHASES 12 & 13: SPARK MLLIB (LR & RANDOM FOREST)

### Command 1: Train & Evaluate Logistic Regression Baseline (Phase 12)
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMULogisticRegression /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Expected Output:**
  ```
  Accuracy           : 0.9153
  Weighted Precision : 0.8630
  Weighted Recall    : 0.9153
  Weighted F1-Score  : 0.8776
  Area Under ROC     : 0.6871
  Confusion Matrix: TP=8, TN=9724, FP=22, FN=878
  FAILED Recall      : 0.0090 (0.90% caught)
  Model saved to: hdfs://namenode:9000/project/models/logistic_regression
  ```
- **Viva Screenshot:** `SS-23`

### Command 2: Train & Evaluate Random Forest Classifier (Phase 13)
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMURandomForest /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Expected Output:**
  ```
  Accuracy           : 0.9228
  Weighted Precision : 0.9048
  Weighted Recall    : 0.9228
  Weighted F1-Score  : 0.9037
  Area Under ROC     : 0.8947
  Confusion Matrix: TP=171, TN=9640, FP=106, FN=715
  FAILED Precision   : 0.6173 (61.73% precision)
  FAILED Recall      : 0.1930 (19.30% caught — 21.4x better than LR)
  Model saved to: hdfs://namenode:9000/project/models/random_forest
  ```
- **Viva Screenshot:** `SS-24`

### Command 3: Verify Model Persistence in HDFS
```powershell
docker exec namenode hdfs dfs -ls -R /project/models
```
- **Purpose:** Proves both models are persisted in HDFS with data, metadata, and Snappy Parquet files.

---

# PART 10 — PHASE 14: K-MEANS ANOMALY DETECTION

### Execute K-Means Anomaly Detection
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUKMeansAnomaly /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Purpose:** Groups 294,649 five-minute windows of HDFS logs into 5 clusters and computes squared Euclidean distance to centroids.
- **Expected Output:** Flags top metadata storm anomaly on `namenode` with score `7584.49` and 108,385 events.
- **Viva Screenshot:** `SS-26`

---

# PART 11 — PHASE 15: STRUCTURED STREAMING REPLAY

### Execute Historical Streaming Replay
```powershell
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.streaming.CMUHistoricalStreaming /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **How to run during demo:**
  1. Let the job start and display `Batch: 0`, `Batch: 1`, `Batch: 2`.
  2. Take screenshot `SS-28` showing micro-batch window outputs.
  3. Press `Ctrl + C` to stop the stream and release worker cores.
- **Viva Evidence:** `SS-28` (`evidence/phase15/SS-28_historical_streaming_output.txt`)

---

# PART 12 — PHASE 16: CONCEPT DRIFT DETECTION

### Execute Drift Detection
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUDriftDetection /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Expected Output:**
  ```
  totalTasks         |     629.73 |     445.08 |     0.08 | STABLE
  mapReduceRatio     |     279.81 |     162.89 |     0.07 | STABLE
  queueWaitMs        |   28749.71 |   22398.08 |     0.01 | STABLE
  ```
- **Viva Screenshot:** `SS-29`

---

# PART 13 — PHASE 17: OPERATIONAL ALERT MONITOR

### Run Alert Monitor
```powershell
python src/python/alert_monitor.py
```
- **Expected Output:** Displays color-coded terminal alerts for K-Means metadata storms and drift warnings, followed by the clearly marked `--- DEMO SIMULATION ---` failure alert.
- **Viva Screenshot:** `SS-30`

---

# PART 14 — PHASE 18: STREAMLIT ANALYTICS DASHBOARD

### Launch Dashboard
```powershell
streamlit run src/python/dashboard.py
```
- **URL:** `http://localhost:8501`
- **What to show in browser:**
  1. **Model Health Cards:** Random Forest Accuracy (92.28%) and ROC-AUC (0.8947).
  2. **Metadata Storm Alerts:** NameNode critical score (7584.49).
  3. **Drift Monitoring Table:** All features showing `STABLE`.
- **Viva Screenshot:** `SS-31`

---

# PART 15 — PHASE 19: PERFORMANCE BENCHMARKING

### Run Benchmarking Suite
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.benchmarking.CMUPerformanceBenchmarker /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
- **Expected Output:** Shows comparative timings across API Overhead (RDD=349ms, DS=270ms, DF=485ms), Partition Scaling (10 partitions optimal at 197ms), and Caching speedup (96ms vs 106ms).
- **Viva Screenshot:** `SS-32`

---

# PART 16 — COMPLETE HDFS DIRECTORY STRUCTURE

```
/project/
├── raw/
│   ├── cmu/                    ← 22.025 GB (20 monthly folders: job, task, attempt, counter, conf, split)
│   └── hdfs_v2/                ← 16.057 GB (31 Hadoop HDFS node log files)
├── processed/
│   ├── cmu_jobs/               ← Snappy Parquet partitioned by label=0 (SUCCESS) and label=1 (FAILED)
│   └── hdfs_logs/              ← Clean parsed log Parquet partitioned by log level
├── batch_results/
│   ├── rdd_status_counts/      ← Phase 6 RDD status counts text output
│   ├── rdd_queue_stats/        ← Phase 6 RDD queue duration statistics
│   ├── dataset_output/         ← Phase 8 typed Dataset Parquet output
│   └── sql_output/             ← Phase 9 SQL engineered features (Input for ML training)
├── mapreduce_output/
│   └── job_status_counts/      ← Phase 5 Java MapReduce part-r-00000 status counts
└── models/
    ├── logistic_regression/    ← Phase 12 persisted Spark MLlib Logistic Regression model
    └── random_forest/          ← Phase 13 persisted Spark MLlib Random Forest model
```

---

# PART 17 — CLUSTER STOP & RESTART PROCEDURES

### Normal Clean Shutdown
```powershell
cd D:\BigData\Project_1\docker
docker-compose down
```
*(Stops all 5 containers while preserving volume data on E: drive).*

### Clean Cluster Restart
```powershell
docker-compose up -d
```

### Restart Spark Services Only
```powershell
docker restart spark-master spark-worker
```

---

# PART 18 — DESTRUCTIVE OPERATIONS WARNING

> [!CAUTION]
> **DO NOT RUN THESE COMMANDS DURING OR BEFORE VIVA!**
> - `docker-compose down -v` — **DELETES ALL HDFS DATA VOLUMES ON DRIVE E:.**
> - `hdfs dfs -rm -r -f /project/raw` — **DELETES THE 38 GB RAW DATASET FROM HDFS.**

---

# PART 19 — TROUBLESHOOTING & RECOVERY RECIPES

### 1. DataNode Not Connecting to NameNode
- **Symptom:** `hdfs dfsadmin -report` shows 0 or 1 Live DataNodes.
- **Diagnosis:** `docker logs datanode1`
- **Fix:** Wait 30 seconds for NameNode healthcheck, then run: `docker restart datanode1 datanode2`.

### 2. Spark Job Stuck in "WAITING" State (Resource Starvation)
- **Symptom:** `TaskSchedulerImpl: Initial job has not accepted any resources`.
- **Cause:** Previous streaming job held all 4 CPU cores on `spark-worker`.
- **Fix:** `docker restart spark-master spark-worker`.

### 3. NameNode Stuck in Safe Mode
- **Symptom:** `SafeModeException: Cannot create directory /project/...`.
- **Fix:** `docker exec namenode hdfs dfsadmin -safemode leave`.

### 4. Port Conflict on Host Machine
- **Symptom:** `Bind for 0.0.0.0:8080 failed: port is already allocated`.
- **Fix:** Stop competing local web servers (e.g., Oracle, Tomcat, IIS).

---

# PART 20 — 40+ VIVA DEFENSE QUESTIONS & ANSWERS

### Core Distributed Systems & Hadoop
1. **Q: What is Docker and why did you use it?**
   - **A:** Docker provides containerized process isolation. We used it to instantiate a complete, multi-node distributed Hadoop (1 NameNode + 2 DataNodes) and Spark (Master + Worker) cluster on a single machine without local software dependency pollution.
2. **Q: How many NameNodes and DataNodes do you have?**
   - **A:** Exactly 1 NameNode (`namenode`) and 2 DataNodes (`datanode1` and `datanode2`).
3. **Q: What is the role of the NameNode?**
   - **A:** The NameNode manages the filesystem namespace, directory tree, permissions, and the mapping of file path chunks to distributed 128 MB block locations across DataNodes. It stores metadata in `fsimage` and `edits` journals.
4. **Q: What is the role of a DataNode?**
   - **A:** DataNodes store physical 128 MB data blocks on disk, perform block creation/deletion upon NameNode instruction, and serve direct read/write streaming pipelines to Spark/MapReduce clients.
5. **Q: What is HDFS replication and what is your replication factor?**
   - **A:** Replication ensures fault tolerance by storing duplicate copies of blocks on different DataNodes. Our replication factor is **2** (`dfs.replication=2`), meaning every block is replicated across both `datanode1` and `datanode2`.
6. **Q: What happens if `datanode1` crashes?**
   - **A:** Because replication is 2, all data blocks remain 100% available on `datanode2`. The NameNode detects missing heartbeats after 10 minutes and flags under-replicated blocks.
7. **Q: Is YARN running in your cluster?**
   - **A:** No. There is no YARN ResourceManager or NodeManager. MapReduce executes in **LOCAL mode** (`mapreduce.framework.name=local`) inside the `namenode` container, while Spark uses its own Standalone Cluster Manager.

### Spark & Core Big Data APIs
8. **Q: What is the difference between RDD, DataFrame, and Dataset?**
   - **A:** 
     - **RDD (Resilient Distributed Dataset):** Low-level immutable collection of JVM objects evaluated lazily without query optimization.
     - **DataFrame:** Untyped `Dataset[Row]` structured collection optimized by the Catalyst query optimizer and Tungsten off-heap execution engine.
     - **Dataset:** Strongly-typed API (`Dataset[T]`) combining type-safety via Scala case classes with Catalyst optimization.
9. **Q: What is Lazy Evaluation in Spark?**
   - **A:** Transformations (e.g., `map`, `filter`, `join`) are not executed immediately; they construct a Directed Acyclic Graph (DAG). Computation only occurs when an Action (e.g., `count`, `save`, `collect`) is called.
10. **Q: Why did you use `.cache()` in your code?**
    - **A:** When a DataFrame or RDD is reused across multiple downstream Actions (such as computing record counts, inspecting schema, and writing to Parquet), caching stores partitions in executor memory to avoid re-reading and re-computing the upstream DAG.
11. **Q: What is the benefit of Parquet format over CSV?**
    - **A:** Parquet is a columnar binary format with Snappy compression and embedded schema. It enables column pruning, predicate pushdown (skipping unneeded row groups), and significantly lower I/O footprint compared to text CSV.

### Machine Learning & Class Imbalance
12. **Q: What is the ML problem and what are you predicting?**
    - **A:** Binary classification predicting whether a distributed Hadoop job will terminate as `SUCCESS` (label=0) or `FAILED` (label=1) based on early submission parameters.
13. **Q: What is class imbalance and what is your class ratio?**
    - **A:** Our dataset contains 49,026 SUCCESS jobs (91.53%) and 4,534 FAILED jobs (8.47%), representing a **10.8 : 1 class imbalance**.
14. **Q: Why is 91.53% Accuracy misleading?**
    - **A:** A trivial model that always predicts SUCCESS achieves 91.53% accuracy but has **0% Recall** on failures, failing completely to alert operators to failing jobs.
15. **Q: What were your Logistic Regression results?**
    - **A:** Accuracy: 91.53%, ROC-AUC: **0.6871**, Minority Failure Recall: **0.90%** (caught only 8 failures). The linear boundary heavily favored the majority class.
16. **Q: What were your Random Forest results?**
    - **A:** Accuracy: 92.28%, ROC-AUC: **0.8947**, Minority Failure Recall: **19.30%** (caught 171 failures), Precision: **61.73%**.
17. **Q: What is the most predictive feature of job failure?**
    - **A:** `totalTasks` (53.54% importance) and `mapReduceRatio` (30.65% importance), proving that job scale and Map/Reduce partition skew are the primary failure drivers.
18. **Q: What is Data Leakage and how did you prevent it?**
    - **A:** Data leakage occurs when post-execution features are used for early prediction. We explicitly dropped `finishTime`, `finMaps`, `finReduces`, `failMaps`, and `failReduces`.

### Anomaly Detection, Streaming & Drift
19. **Q: Why did you use K-Means for HDFS logs instead of Random Forest?**
    - **A:** The 16 GB HDFS_v2 dataset contains no ground-truth failure labels. Unsupervised K-Means clustering is required to group 5-minute event patterns and detect statistical outliers.
20. **Q: How is the K-Means anomaly score calculated?**
    - **A:** As the **squared Euclidean distance** from a 5-minute window's feature vector to its assigned cluster centroid. Windows with score $> 3000.0$ represent extreme metadata storms.
21. **Q: What is your Structured Streaming implementation?**
    - **A:** It is a **Historical Log Replay** that ingests production HDFS log files in micro-batches (`maxFilesPerTrigger=1`) over 5-minute tumbling windows with a 10-minute watermark.
22. **Q: What is Concept Drift?**
    - **A:** A shift in the statistical distribution of input features over time. We track this using absolute Z-scores comparing a baseline window (70%) to a monitoring window (30%). All Z-scores were $< 0.1$, confirming stability.

---

# QUICK COMMAND CHEAT SHEET

| Category | Goal / Action | Exact PowerShell / Terminal Command |
|---|---|---|
| **Startup** | Start cluster | `cd D:\BigData\Project_1\docker; docker-compose up -d` |
| **Startup** | Check container health | `docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"` |
| **HDFS** | 2 DataNodes report | `docker exec namenode hdfs dfsadmin -report` |
| **HDFS** | Check raw data volume | `docker exec namenode hdfs dfs -du -s -h /project/raw/*` |
| **HDFS** | Check processed data | `docker exec namenode hdfs dfs -ls /project/processed/cmu_jobs` |
| **HDFS** | Check persisted models | `docker exec namenode hdfs dfs -ls -R /project/models` |
| **Pipeline** | Run entire batch pipeline | `powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1` |
| **MapReduce** | Run Java MapReduce | `docker exec namenode bash -c "time hadoop jar /opt/hadoop-apps/job-status-mr.jar edu.project1.mapreduce.JobStatusDriver"` |
| **Spark RDD** | Run RDD Job | `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.rdd.RDDJobParser /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` |
| **Spark ML** | Run Logistic Regression | `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMULogisticRegression /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` |
| **Spark ML** | Run Random Forest | `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMURandomForest /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` |
| **Anomaly** | Run K-Means Anomaly | `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUKMeansAnomaly /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` |
| **Streaming** | Run Structured Streaming | `docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.streaming.CMUHistoricalStreaming /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` |
| **Drift** | Run Drift Monitor | `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUDriftDetection /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` |
| **Alerts** | Run Alert Monitor | `python src/python/alert_monitor.py` |
| **Dashboard** | Start Web UI | `streamlit run src/python/dashboard.py` (Open `http://localhost:8501`) |
| **Benchmark** | Run Performance Benchmark | `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.benchmarking.CMUPerformanceBenchmarker /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` |
| **Shutdown** | Clean shutdown | `cd D:\BigData\Project_1\docker; docker-compose down` |
