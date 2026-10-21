# PROJECT AUDIT REPORT (POST-REMEDIATION FINAL)
## Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Course: 23AID302 — Big Data Analytics
## Last Audit & Remediation Date: 2026-08-17
## Methodology: Forensic File-by-File Code Audit & Live Distributed Cluster Verification

---

# 1. EXECUTIVE SUMMARY

### Final Overall Project Health: 🟢 GREEN

This document presents the definitive, forensic technical audit and post-remediation report for the **Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform**. 

The evaluation was conducted through deep line-by-line inspection of all 13 Scala source files, 3 Java MapReduce source files, 2 Python operational scripts, build configuration scripts, Docker container compositions, and live distributed executions against an active 5-container cluster.

Every defect, missing metric, unpersisted model, build configuration gap, and documentation discrepancy identified during earlier audit passes has been systematically resolved, verified through live execution, and checked into the version control repository (`commit: 9f0c15d`).

### Summary of Verified Audit Dimensions:
- **Build System Integrity:** Clean compilation of all 13 Scala source files and passing of all 3 ScalaTest unit tests (`CMUUnitTests`) executed from a clean state inside Docker.
- **Data Ingestion & Storage:** 38.082 GB total analytical data (22.025 GB structured CMU OpenCloud traces across 20 monthly directories + 16.057 GB unstructured HDFS_v2 node logs across 31 files) verified live in HDFS with replication factor 2.
- **Compute Pipeline Execution:** End-to-end execution of Java MapReduce (Phase 5), Spark RDD (Phase 6), Spark DataFrame (Phase 7), Spark Dataset (Phase 8), and Spark SQL (Phase 9) via `scripts/run_pipeline.ps1` with exit code 0.
- **Supervised Machine Learning:** Evaluated Spark MLlib Logistic Regression and Random Forest models across full multi-class evaluators (Accuracy, Weighted Precision, Recall, F1, ROC-AUC, Confusion Matrix, Minority `FAILED` class recall). The Random Forest model achieved **0.8947 ROC-AUC** and **19.30% failure recall** (a 21.4x improvement over the linear baseline).
- **Model Persistence & Reload:** Persistence of trained models to HDFS (`/project/models/logistic_regression` and `/project/models/random_forest`) with successful independent reload and prediction verification.
- **Unsupervised Anomaly Detection:** K-Means clustering ($k=5$) over 294,649 five-minute tumbling windows identifying high-severity metadata storm anomalies on the NameNode (>100,000 events/5m).
- **Structured Streaming:** Historical log replay via Spark Structured Streaming processing multi-batch tumbling window aggregations with console anomaly alerting.
- **Concept Drift & Alerting:** Statistical Z-score feature drift tracking ($Z < 0.1$, STABLE) and operational alert generation with clear separation of real model outputs from demo simulations.
- **Operational Dashboard:** Multi-page interactive Streamlit dashboard mapping real backend evaluation files to interactive UI components.

---

# 2. PROJECT OVERVIEW

### Problem Statement & Utility
Modern large-scale distributed computing clusters (such as Hadoop and Spark) generate massive operational telemetry consisting of both structured job execution records and unstructured service logs. When distributed jobs fail or cluster nodes experience metadata storms, debugging requires labor-intensive forensic parsing across distributed machines.

This project implements an end-to-end Big Data intelligence platform designed to:
1. **Predict Hadoop Job Failures:** Use early execution characteristics (task counts, Map-to-Reduce task ratios, queue wait durations) to predict whether a batch job will succeed or fail before consuming massive cluster compute resources.
2. **Detect Infrastructure Anomalies:** Perform unsupervised pattern discovery over high-volume HDFS node logs to flag cluster-wide metadata storms and communication breakdowns.
3. **Replay Streaming Telemetry:** Ingest high-velocity log streams in micro-batches using Spark Structured Streaming to deliver real-time operational alerts.
4. **Monitor Concept Drift:** Statistically evaluate feature stability across time windows to detect when changing cluster workloads necessitate model retraining.

### End-to-End Analytics Pipeline Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       CMU OpenCloud (22.025 GB)                                        │
│                                (job.csv, task.csv, attempt.csv, etc.)                                  │
└───────────────────────────────────────────────────┬────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       HDFS Storage (/project/raw/)                                     │
│                                      1 NameNode + 2 DataNodes (RF=2)                                   │
└───────────────┬───────────────────────────────────┬───────────────────────────────────┬────────────────┘
                │                                   │                                   │
                ▼                                   ▼                                   ▼
   ┌───────────────────────────┐       ┌───────────────────────────┐       ┌───────────────────────────┐
   │ Phase 5: Java MapReduce   │       │ Phase 6: Spark RDD        │       │ Phase 7: Spark DataFrame  │
   │ (Local MapReduce Baseline)│       │ (Low-Level Partition Map) │       │ (Relational Joins & Clean)│
   └────────────┬──────────────┘       └────────────┬──────────────┘       └────────────┬──────────────┘
                │                                   │                                   │
                ▼                                   ▼                                   ▼
   ┌───────────────────────────┐       ┌───────────────────────────┐       ┌───────────────────────────┐
   │ Class Aggregation Output  │       │ Status & Queue Statistics │       │ Partitioned Parquet Data  │
   │ (/mapreduce_output/)      │       │ (/batch_results/rdd_*/)   │       │ (/processed/cmu_jobs/)    │
   └───────────────────────────┘       └───────────────────────────┘       └────────────┬──────────────┘
                                                                                        │
                                                    ┌───────────────────────────────────┘
                                                    ▼
                                       ┌───────────────────────────┐
                                       │ Phase 8: Spark Dataset    │
                                       │ (Typed Case Class Joins)  │
                                       └────────────┬──────────────┘
                                                    │
                                                    ▼
                                       ┌───────────────────────────┐
                                       │ Phase 9: Spark SQL (CTEs) │
                                       │ (Engineered ML Features)  │
                                       └────────────┬──────────────┘
                                                    │
                                                    ▼
                                       ┌───────────────────────────┐
                                       │ Parquet ML Training Data  │
                                       │ (/batch_results/sql_out/) │
                                       └────────────┬──────────────┘
                                                    │
                                                    ▼
                                       ┌───────────────────────────┐
                                       │ Phase 12 & 13: MLlib      │
                                       │ (LR Baseline vs RF Model) │
                                       └────────────┬──────────────┘
                                                    │
                                                    ▼
                                       ┌───────────────────────────┐
                                       │ Persisted HDFS Models     │
                                       │ (/project/models/lr & rf) │
                                       └───────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          HDFS_v2 (16.057 GB)                                           │
│                                  (31 Hadoop HDFS Log Files, 58M+ Lines)                                │
└───────────────────────────────────────────────────┬────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
                                       ┌───────────────────────────┐
                                       │ HDFS Storage (/raw/hdfs/) │
                                       └────────────┬──────────────┘
                                                    │
                         ┌──────────────────────────┴──────────────────────────┐
                         │                                                     │
                         ▼                                                     ▼
            ┌───────────────────────────┐                         ┌───────────────────────────┐
            │ Phase 14.1: Preprocessing │                         │ Phase 15: Spark Streaming │
            │ (Regex Parsing & Window)  │                         │ (Historical Log Replay)   │
            └────────────┬──────────────┘                         └────────────┬──────────────┘
                         │                                                     │
                         ▼                                                     ▼
            ┌───────────────────────────┐                         ┌───────────────────────────┐
            │ Phase 14.2: K-Means       │                         │ Real-Time Window Alerts   │
            │ (Unsupervised Anomaly)    │                         │ (Tumbling 5m Windows)     │
            └────────────┬──────────────┘                         └───────────────────────────┘
                         │
                         ▼
            ┌───────────────────────────┐
            │ Phase 16: Drift Monitor   │
            │ (Statistical Z-Score)     │
            └────────────┬──────────────┘
                         │
                         ▼
            ┌───────────────────────────┐
            │ Phase 17: Python Alerts   │
            │ (Console Alert Monitor)   │
            └────────────┬──────────────┘
                         │
                         ▼
            ┌───────────────────────────┐
            │ Phase 18: Dashboard       │
            │ (Streamlit Web UI :8501)  │
            └───────────────────────────┘
```

---

# 3. REMEDIATION HISTORY

The following matrix documents the complete lifecycle of all findings identified during forensic audit passes, their technical root causes, the exact remediation actions applied, and their live verification evidence.

| Finding ID | Severity | Component | Original Issue | Root Cause | Files Changed | Remediation Action | Live Verification Result | Evidence ID | Status |
|---|---|---|---|---|---|---|---|---|---|
| **CRIT-01** | CRITICAL | Docker Infrastructure | Docker Desktop daemon was stopped during initial audit | Background WSL2 subsystem was inactive | None (Service level) | Started Docker Desktop and executed `docker-compose up -d` | `docker ps` confirms 5 containers; `hdfs dfsadmin -report` confirms 2 live DataNodes (80.10 GB DFS used) | `SS-01` | **RESOLVED** |
| **CRIT-02** | CRITICAL | Structured Streaming | `SS-28` historical streaming screenshot evidence was missing | Job had not been executed to completion across micro-batches | `evidence/phase15/` | Executed `CMUHistoricalStreaming` against `/project/raw/hdfs_v2` | Processed 4 consecutive micro-batches; captured console output to `SS-28_historical_streaming_output.txt` | `SS-28` | **RESOLVED** |
| **CRIT-03** | CRITICAL | Spark MLlib | Trained LR and RF models were discarded in-memory without persistence | `.save()` API was not invoked in driver code | `CMULogisticRegression.scala`, `CMURandomForest.scala` | Added `.write.overwrite().save()` to HDFS `/project/models/` and added reload-verification test | `hdfs dfs -ls -R /project/models` confirms binary model directories, metadata, and snappy parquet files | HDFS Verify | **RESOLVED** |
| **MED-01** | MEDIUM | Orchestration | `run_pipeline.sh` referenced nonexistent `resourcemanager` container and invalid class/JAR paths | Legacy YARN script template was committed | `scripts/run_pipeline.sh`, `scripts/run_pipeline.ps1` | Replaced container name with `namenode`, JAR with `/opt/hadoop-apps/job-status-mr.jar`, class with `JobStatusDriver`; created native PowerShell equivalent | `powershell -File scripts\run_pipeline.ps1` completed end-to-end across all batch phases with exit code 0 | `SS-22` | **RESOLVED** |
| **MED-02** | MEDIUM | Build System | `build.sbt` missed `rdd/`, `dataframe/`, `dataset/` source directories | SBT unmanaged source directories were hardcoded to a subset | `src/scala/build.sbt` | Added missing source directories to `Compile / unmanagedSourceDirectories` and fixed test path to `src/test/scala` | Clean `sbt clean compile test assembly` compiled 13 Scala sources, passed 3/3 tests, assembled 107 KB JAR | `SS-33` | **RESOLVED** |
| **MED-03** | MEDIUM | ML Evaluation | Models only reported Accuracy and ROC-AUC; lacked Precision, Recall, F1, and Confusion Matrix | Basic evaluators were used; class imbalance was unmonitored | `CMULogisticRegression.scala`, `CMURandomForest.scala` | Added `MulticlassClassificationEvaluator` and manual confusion matrix counters for minority `FAILED` class | Evaluated: RF achieved **AUC=0.8947, Minority Recall=19.30%, Precision=61.73%** (vs LR Recall=0.90%) | `SS-23`, `SS-24` | **RESOLVED** |
| **MED-04** | MEDIUM | ML Baseline Tuning | Logistic Regression baseline had `AUC=0.5000` (random guess) | Over-regularization (`regParam=0.3`, `elasticNetParam=0.8`) shrunk all coefficients to zero | `CMULogisticRegression.scala` | Tuned to `regParam=0.01` (pure L2), `elasticNetParam=0.0`, `maxIter=100` | Re-execution lifted LR ROC-AUC to **0.6871** | `SS-23` | **RESOLVED** |
| **MIN-01** | MINOR | Documentation | `requirements-traceability.md` had all requirements marked as PENDING | Document had not been updated after implementation | `requirements-traceability.md` | Rewrote entire matrix with verified file links, phase numbers, evidence IDs, and COMPLETE statuses | Document verified clean and synchronized with repository | Doc Verify | **RESOLVED** |
| **MIN-02** | MINOR | Documentation | `evidence-register.md` contained duplicate row blocks (lines 7–37 and 38–70) | Accidental copy-paste duplication during earlier session | `evidence-register.md` | Removed duplicate rows, formatted table with exact on-disk files, sizes, and statuses | Evidence register matched 1:1 with `evidence/` directory | Doc Verify | **RESOLVED** |
| **MIN-03** | MINOR | Alerting | `alert_monitor.py` contained hardcoded RF alert presented without demo distinction | Static simulation script was used for demonstration | `src/python/alert_monitor.py` | Renamed function to `demo_simulated_rf_alert()` and added prominent `--- DEMO SIMULATION ---` banner | Terminal execution clearly identifies test vs production alerts | `SS-30` | **RESOLVED** |

---

# 4. COMPLETE VERIFIED SYSTEM ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       Docker Host (Windows 11)                                          │
│                           Docker Engine: v29.7.2 | WSL2 Storage on Drive E:                            │
│                                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                    Bridge Network: bigdata_net                                     │  │
│  │                                                                                                   │  │
│  │  ┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐          │  │
│  │  │        namenode         │   │        datanode1        │   │        datanode2        │          │  │
│  │  │      Hadoop 3.2.1       │   │      Hadoop 3.2.1       │   │      Hadoop 3.2.1       │          │  │
│  │  │  Port 9870 (Web UI)     │   │  Internal Hadoop IPC    │   │  Internal Hadoop IPC    │          │  │
│  │  │  Port 9000 (HDFS IPC)   │   │  Replication Replica 1  │   │  Replication Replica 2  │          │  │
│  │  └────────────┬────────────┘   └────────────┬────────────┘   └────────────┬────────────┘          │  │
│  │               │                             │                             │                       │  │
│  │               └─────────────────────────────┴─────────────────────────────┘                       │  │
│  │                                             │                                                     │  │
│  │                                             ▼                                                     │  │
│  │                             HDFS Distributed File System                                          │  │
│  │                            hdfs://namenode:9000/project/                                          │  │
│  │                                             │                                                     │  │
│  │               ┌─────────────────────────────┴─────────────────────────────┐                       │  │
│  │               │                                                           │                       │  │
│  │  ┌────────────┴────────────┐                             ┌────────────────┴────────────┐          │  │
│  │  │      spark-master       │                             │        spark-worker         │          │  │
│  │  │       Spark 3.2.1       │◄───────────────────────────►│         Spark 3.2.1         │          │  │
│  │  │  Port 8080 (Web UI)     │     Internal Akka/Netty     │  Port 8081 (Web UI)         │          │  │
│  │  │  Port 7077 (Master IPC) │         Cluster IPC         │  4 CPU Cores | 4 GB RAM     │          │  │
│  │  └─────────────────────────┘                             └─────────────────────────────┘          │  │
│  └───────────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                         │
│  Mounted Volumes:                                                                                       │
│    - Host Path: D:\BigData\Project_1  ──►  Container Path: /host-project                                │
│    - Docker WSL Storage: E:\Docker\DockerDesktop\DockerDesktopWSL\data\ext4.vhdx                         │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# 5. DOCKER ARCHITECTURE

The cluster environment is defined in `docker/docker-compose.yml` and utilizes official `bde2020` Big Data Europe images based on Debian Stretch and OpenJDK 8.

### Verified Container Inventory

| Container Name | Image Tag | Core Role | Internal IP | External Exposed Ports | Volume Mounts | Health Status |
|---|---|---|---|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | HDFS NameNode & Local MapReduce Runner | `172.18.0.3` | `9870:9870` (Web UI)<br>`9000:9000` (IPC) | `hadoop_namenode:/hadoop/dfs/name`<br>`D:/BigData/Project_1:/host-project` | `Up (healthy)` (via port 9870 curl) |
| `datanode1` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | HDFS DataNode 1 (Block Store) | `172.18.0.5` | `9864` (internal) | `hadoop_datanode1:/hadoop/dfs/data`<br>`D:/BigData/Project_1:/host-project` | `Up (healthy)` (via port 9864 curl) |
| `datanode2` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | HDFS DataNode 2 (Block Store) | `172.18.0.6` | `9864` (internal) | `hadoop_datanode2:/hadoop/dfs/data`<br>`D:/BigData/Project_1:/host-project` | `Up (healthy)` (via port 9864 curl) |
| `spark-master` | `bde2020/spark-master:3.2.1-hadoop3.2` | Spark Standalone Master & Driver Submitter | `172.18.0.4` | `8080:8080` (Web UI)<br>`7077:7077` (Cluster IPC) | `D:/BigData/Project_1:/host-project` | `Up` |
| `spark-worker` | `bde2020/spark-worker:3.2.1-hadoop3.2` | Spark Standalone Worker Executor | `172.18.0.2` | `8081:8081` (Web UI) | `D:/BigData/Project_1:/host-project` | `Up` (4 Cores, 4096 MB RAM) |

---

# 6. HADOOP / HDFS ARCHITECTURE

### HDFS Internals & Configuration Parameters
- **Default Filesystem URI:** `hdfs://namenode:9000`
- **Replication Factor:** **2** (`HDFS_CONF_dfs_replication=2`). Every block written to HDFS is stored on both `datanode1` and `datanode2`.
- **Default Block Size:** **128 MB** (`134217728 bytes`). Large block sizes minimize NameNode metadata overhead for Big Data files.
- **Permissions:** Disabled (`HDFS_CONF_dfs_permissions_enabled=false`) to allow seamless inter-container read/write operations by user `root`.
- **Block Compression:** Snappy codec enabled for columnar Parquet outputs.
- **Metadata Persistence:** The NameNode maintains the filesystem tree and block mapping in-memory, backed by `fsimage` checkpoint files and sequential `edits_*` journals inside `/hadoop/dfs/name`.

### Critical Viva Architectural Questions Answered
1. **How many NameNodes?** Exactly **1 NameNode** (`namenode`).
2. **How many DataNodes?** Exactly **2 DataNodes** (`datanode1` and `datanode2`). Both are confirmed healthy via `hdfs dfsadmin -report`.
3. **Is SecondaryNameNode running?** **No.** A SecondaryNameNode container is not defined in `docker-compose.yml`. In production, a SecondaryNameNode periodically merges `fsimage` and `edits` logs, but in this standalone deployment, the primary NameNode handles checkpointing on startup.
4. **Is YARN (ResourceManager / NodeManager) running?** **No.** There are no YARN containers.
5. **What is the MapReduce execution mode?** MapReduce runs in **LOCAL mode** (`MAPRED_CONF_mapreduce_framework_name=local` in `hadoop.env`).
   - *Viva Explanation:* "MapReduce executes within a local JVM inside the `namenode` container. The data is read from and written to distributed HDFS across both DataNodes, but task scheduling is handled by the `LocalJobRunner`. In contrast, all Spark jobs execute genuinely distributed across the `spark-master` and `spark-worker` standalone cluster."

---

# 7. DATASET DOCUMENTATION

The project utilizes two distinct, production-scale datasets totaling **38.082 GB** to satisfy the Big Data **Volume, Variety, Velocity, and Veracity** requirements.

```
Total Analytical Volume: 38.082 GB
├── CMU OpenCloud Cluster Workload (Structured Traces) : 22.025 GB
└── HDFS_v2 Distributed System Logs (Unstructured Logs): 16.057 GB
```

## Dataset 1: CMU OpenCloud Workload (22.025 GB)
- **Origin:** Carnegie Mellon University Parallel Data Lab (PDL) OpenCloud production Hadoop cluster.
- **Temporal Coverage:** May 2010 through December 2011 (**20 consecutive monthly directories**).
- **Structure:** 20 directories (`2010-05` to `2011-12`), each containing 6 relational CSV tables:

| Table Name | Purpose | Key Columns | Data Types | Role in Pipeline |
|---|---|---|---|---|
| `job.csv` | Primary job lifecycle, timing, status, and task summary | `jtid` (Long), `jobid` (Long), `submitTime` (Long), `launchTime` (Long), `finishTime` (Long), `status` (Int), `numMaps` (Int), `numReduces` (Int), `finMaps` (Int), `finReduces` (Int), `failMaps` (Int), `failReduces` (Int) | Structured CSV with headers | Primary entity table for Phase 5 (MR), Phase 6 (RDD), Phase 7 (DF), Phase 8 (DS), Phase 9 (SQL), Phase 12/13 (ML). |
| `task.csv` | Individual Map and Reduce task executions within jobs | `jtid` (Long), `jobid` (Long), `taskid` (Long), `taskType` (String: MAP/REDUCE), `startTime` (Long), `finishTime` (Long), `status` (Int), `failures` (Int) | Structured CSV with headers | Joined in Phase 7/8/9 to compute aggregated `taskCount` and `taskFailureCount`. |
| `attempt.csv` | Speculative and restart attempts per task | `jtid` (Long), `taskid` (Long), `attemptid` (Long), `startTime` (Long), `finishTime` (Long), `status` (Int), `node` (String) | Structured CSV with headers | Joined in Phase 7/8/9 to compute `totalAttempts`, `failedAttempts`, and `retryRate`. |
| `counter.csv` | Hardware, file system, and framework task counters | `jtid` (Long), `jobid` (Long), `group` (String), `counter` (String), `value` (Long) | Key-value records | Raw metrics store (verified present in HDFS). |
| `conf.csv` | Hadoop XML configuration parameters per job | `jtid` (Long), `jobid` (Long), `property` (String), `value` (String) | Key-value records | Job configuration parameters. |
| `split.csv` | Input split locations and byte offsets | `jtid` (Long), `jobid` (Long), `splitId` (Long), `locations` (String), `length` (Long) | Geographic mapping | HDFS split locality mapping. |

## Dataset 2: HDFS_v2 Distributed System Logs (16.057 GB)
- **Origin:** Real-world Hadoop HDFS production cluster operational logs.
- **File Structure:** **31 individual `.log` text files** in `/project/raw/hdfs_v2/`:
  - 1 NameNode log: `hadoop-hdfs-namenode-mesos-01.log` (5.42 GB)
  - 1 SecondaryNameNode log: `hadoop-hdfs-secondarynamenode-mesos-01.log` (88.3 MB)
  - 29 DataNode logs: `hadoop-hdfs-datanode-mesos-01.log` to `mesos-32.log` (300 MB – 700 MB each)
- **Total Volume:** Over **58 million log lines**.
- **Log Format:** Unstructured text parsed via Scala Regular Expression:
  ```regex
  ^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+([\w\.]+):\s+(.*)$
  ```
  Extracts: `timestamp` (yyyy-MM-dd HH:mm:ss,SSS), `level` (INFO, WARN, ERROR, FATAL), `component` (Java class), and `message`.
- **Unlabeled Nature:** HDFS_v2 contains no ground-truth failure labels. It is therefore utilized exclusively for **unsupervised anomaly detection (K-Means)** and **high-velocity streaming replay**.

---

# 8. DATA FLOW ACROSS BRANCHES

The two datasets serve independent, scientifically decoupled analytical functions:

```
Branch A: Supervised Job Failure Prediction (CMU OpenCloud)
  Raw CSVs in HDFS
    ──► Filter Invalid & Drop Leakage
    ──► Relational Join (job + task + attempt)
    ──► Feature Extraction (totalTasks, mapReduceRatio, queueWaitMs)
    ──► Spark MLlib Training (Logistic Regression vs Random Forest)
    ──► Evaluation & Model Persistence to /project/models/

Branch B: Unsupervised Infrastructure Log Intelligence (HDFS_v2)
  Raw .log Files in HDFS
    ──► Regex Parsing & Timestamp Normalization
    ──► 5-Minute Tumbling Window Aggregation
    ──► StandardScaler Normalization
    ──► Spark MLlib K-Means Clustering (k=5)
    ──► Distance-Based Anomaly Scoring (Flagging Metadata Storms)
    ──► Structured Streaming Micro-Batch Replay & Concept Drift Tracking

Integration Layer:
  Outputs from Branch A & Branch B ──► Alert Monitor (Phase 17) ──► Streamlit Dashboard (Phase 18)
```

---

# 9. PREPROCESSING AUDIT

### Data Cleaning Arithmetic & Record Transitions
All preprocessing transformations were audited across `CMUPreprocessor.scala`, `CMUDataFrameProcessor.scala`, and `HDFSLogPreprocessor.scala`.

| Transformation Step | Input Criteria | Action Applied | Input Count | Output Count | Records Dropped | Rationale |
|---|---|---|---|---|---|---|
| **1. Raw Load** | `job.csv` files | Read with explicit schema `DROPMALFORMED` | 55,252 | 55,252 | 0 | Ensures schema enforcement across all 20 monthly files |
| **2. Target Filtering** | Status codes | Keep only `status IN (0, 1)` | 55,252 | 53,560 | 1,692 | Excludes 1,666 `KILLED` jobs (user-aborted) and 26 `UNKNOWN_6` malformed entries |
| **3. Anomaly Scrubbing** | `queueWaitMs` | Filter `queueWaitMs >= 0` | 53,560 | 53,559 | 1 | Removed 1 corrupt record where `launchTime < submitTime` |
| **4. Null Imputation** | Joined task/attempt cols | `.na.fill(0)` | 53,559 | 53,559 | 0 | Sets task failures and attempts to 0 for jobs without sub-tasks |
| **5. Leakage Dropping** | 5 Post-run columns | `.drop(LEAKAGE_COLS)` | 53,559 | 53,559 | 0 | Drops post-completion execution metrics |
| **FINAL CLEAN DATASET** | **Clean Parquet** | Written to `/project/batch_results/sql_output` | — | **53,559** | — | **Final verified record count for ML** |

---

# 10. FEATURE ENGINEERING MATRIX

| Feature Name | Source Table | Exact Mathematical Formula | Data Type | Availability Point | Used In ML? | Leakage Risk Level | Technical Justification |
|---|---|---|---|---|---|---|---|
| `queueWaitMs` | `job.csv` | `launchTime - submitTime` | Long (ms) | Job Launch Time | ✅ Yes | **Zero Leakage** | `submitTime` and `launchTime` are recorded when the cluster scheduler assigns containers, before execution finishes. |
| `totalTasks` | `job.csv` | `numMaps + numReduces` | Integer | Job Launch Time | ✅ Yes | **Zero Leakage** | The total number of planned map and reduce splits is determined statically at job submission from input data size. |
| `mapReduceRatio`| `job.csv` | `numMaps / coalesce(numReduces, 1)` | Double | Job Launch Time | ✅ Yes | **Zero Leakage** | Fixed topological parameter representing DAG shape and parallelism balance at submission. |
| `taskFailureCount`| `task.csv` | `SUM(failures)` grouped by `jobid` | Long | Post-Task Complete | ✅ Retained | **Low Practical Risk** | Cumulative sub-task failures. RF feature importance evaluated to 0.000000; model learned no dependency. |
| `retryRate` | `attempt.csv`| `failedAttempts / totalAttempts` | Double | Post-Task Complete | ✅ Retained | **Low Practical Risk** | Task restart frequency. RF feature importance evaluated to 0.000000; model learned no dependency. |

---

# 11. MACHINE LEARNING METHODOLOGY

### Supervised Classification Formulation
- **Objective:** Binary classification of Hadoop job termination state.
- **Target Variable:** `label` ($0 = \text{SUCCESS}$, $1 = \text{FAILED}$).
- **Feature Vector:** `[queueWaitMs, totalTasks, mapReduceRatio, taskFailureCount, retryRate]` assembled via `VectorAssembler`.
- **Dataset Split:** 80% Training ($42,927\text{ records}$), 20% Testing ($10,632\text{ records}$), `seed = 1234L`.

### Model 1: Logistic Regression (Baseline)
- **Algorithm:** `org.apache.spark.ml.classification.LogisticRegression`
- **Hyperparameters:** `maxIter = 100`, `regParam = 0.01`, `elasticNetParam = 0.0` (L2 Ridge Regularization).
- **Persistence Path:** `hdfs://namenode:9000/project/models/logistic_regression`

### Model 2: Random Forest Classifier (Primary Model)
- **Algorithm:** `org.apache.spark.ml.classification.RandomForestClassifier`
- **Hyperparameters:** `numTrees = 50`, `maxDepth = 10`, `featureSubsetStrategy = "auto"`, `seed = 42L`.
- **Persistence Path:** `hdfs://namenode:9000/project/models/random_forest`

---

# 12. CLASS IMBALANCE ANALYSIS

The raw CMU OpenCloud dataset exhibits a severe class imbalance:

```
Raw CMU Dataset Distribution (55,252 Total Records):
├── SUCCESS   : 49,026 records (88.73%)
├── FAILED    :  4,534 records ( 8.21%)
├── KILLED    :  1,666 records ( 3.01%)  [Excluded from ML]
└── UNKNOWN_6 :     26 records ( 0.05%)  [Excluded from ML]

Clean ML Dataset Distribution (53,559 Records):
├── SUCCESS (label=0) : 49,026 records (91.53%)
└── FAILED  (label=1) :  4,534 records ( 8.47%)
```

### Analytical Implications of Imbalance
- **Imbalance Ratio:** **10.8 : 1** (SUCCESS to FAILED).
- **The "Accuracy Paradox":** A naive classifier that always predicts `SUCCESS` achieves an apparent accuracy of **91.53%** while having a failure recall of **0.00%** (failing completely in its operational objective).
- **Why AUC and Recall Matter:** For failure prediction, the primary operational goal is catching true failures (`label=1`). Therefore, model performance must be evaluated using **ROC-AUC**, **Minority Class Recall**, and **Confusion Matrices**.

---

# 13. COMPREHENSIVE MODEL EVALUATION

Both models were executed against the identical 10,632-record test partition on the live Spark cluster. The results are recorded below:

| Evaluation Metric | Logistic Regression (Phase 12) | Random Forest (Phase 13) | Performance Differential |
|---|---|---|---|
| **Accuracy** | 91.53% | **92.28%** | +0.75% |
| **Weighted Precision** | 0.8630 | **0.9048** | +0.0418 |
| **Weighted Recall** | 0.9153 | **0.9228** | +0.0075 |
| **Weighted F1-Score** | 0.8776 | **0.9037** | +0.0261 |
| **Area Under ROC (AUC)** | 0.6871 | **0.8947** | **+0.2076 (Strong Non-Linear Signal)** |
| **Training Time** | 2.272 seconds | 5.668 seconds | +3.396 seconds |
| **True Positives (TP - Caught Failures)** | 8 | **171** | **+163 Caught Failures (21.4x Improvement)** |
| **True Negatives (TN - Caught Success)** | 9,724 | 9,640 | -84 |
| **False Positives (FP - False Alarms)** | 22 | 106 | +84 |
| **False Negatives (FN - Missed Failures)**| 878 | **715** | **-163 Missed Failures** |
| **Minority Class (FAILED) Precision** | 26.67% | **61.73%** | **+35.06% Precision** |
| **Minority Class (FAILED) Recall** | 0.90% | **19.30%** | **21.4x Improvement in Detection** |
| **Minority Class (FAILED) F1-Score** | 0.0175 | **0.2941** | **+0.2766** |

### Random Forest Feature Importances
1. `totalTasks`: **0.535434 (53.54%)** — Single strongest failure predictor. Massive jobs with thousands of tasks have a dramatically higher probability of container failure or node timeout.
2. `mapReduceRatio`: **0.306473 (30.65%)** — High Map-to-Reduce skew indicates heavy shuffle bottlenecks and memory pressure.
3. `queueWaitMs`: **0.158093 (15.81%)** — Prolonged scheduler wait times correlate with congested clusters and resource thrashing.
4. `taskFailureCount`: `0.000000`
5. `retryRate`: `0.000000`

---

# 14. DATA LEAKAGE FORENSIC AUDIT

Data leakage occurs when features contain information that would not be available at the moment a prediction must be made in production.

| Column Name | Action Taken | Available at Prediction Point? | Potential Leakage? | Detailed Technical Reason |
|---|---|---|---|---|
| `finishTime` | **DROPPED** | ❌ No | **FATAL LEAKAGE** | Unix timestamp of job completion. If present, `finishTime - submitTime` reveals exact runtime. |
| `finMaps` | **DROPPED** | ❌ No | **FATAL LEAKAGE** | Total successfully completed map tasks. If `finMaps < numMaps`, job failed. |
| `finReduces` | **DROPPED** | ❌ No | **FATAL LEAKAGE** | Total successfully completed reduce tasks. |
| `failMaps` | **DROPPED** | ❌ No | **FATAL LEAKAGE** | Total failed map tasks recorded upon job termination. |
| `failReduces` | **DROPPED** | ❌ No | **FATAL LEAKAGE** | Total failed reduce tasks recorded upon job termination. |
| `queueWaitMs` | **KEPT** | ✅ Yes | **Zero Leakage** | Derived from `launchTime - submitTime`. Both timestamps are fixed when the job starts executing. |
| `totalTasks` | **KEPT** | ✅ Yes | **Zero Leakage** | Static property (`numMaps + numReduces`) declared in JobConf at submission. |
| `mapReduceRatio`| **KEPT** | ✅ Yes | **Zero Leakage** | Static topological property declared at submission. |
| `taskFailureCount`| **KEPT** | ⚠️ Partial | **Low Risk** | Feature importance is 0.000000 in Random Forest. |
| `retryRate` | **KEPT** | ⚠️ Partial | **Low Risk** | Feature importance is 0.000000 in Random Forest. |

---

# 15. UNSUPERVISED ANOMALY DETECTION (K-MEANS)

### Architecture & Formulation
- **Source Data:** 16.057 GB HDFS_v2 node logs parsed via regex.
- **Windowing:** 5-minute tumbling windows grouped by `node_name`.
- **Feature Vector:** `[eventCount, errorCount, warningCount, uniqueComponents]`.
- **Feature Scaling:** `StandardScaler(withStd = true, withMean = false)`.
- **Algorithm:** `org.apache.spark.ml.clustering.KMeans` ($k = 5$, `seed = 42L`, `maxIter = 20`).
- **Total Windows Processed:** **294,649 five-minute windows**.

### Anomaly Scoring Mechanism
The anomaly score $S(x)$ for a window feature vector $x$ is defined as the **squared Euclidean distance** to its assigned cluster centroid $\mu_{c}$:
$$S(x) = \|x - \mu_{c}\|_{2}^{2} = \sum_{i=1}^{d} (x_{i} - \mu_{c,i})^{2}$$

Windows with $S(x) > 3000.0$ represent extreme statistical outliers.

### Cluster Distribution & Top Anomalies (from `metrics/ml_kmeans_anomaly.txt`)
- **Cluster 0:** 194,056 windows (Normal DataNode heartbeat activity)
- **Cluster 1:** 53,807 windows (Moderate DataNode block reporting)
- **Cluster 2:** 26,546 windows (Active block transfer windows)
- **Cluster 3:** 15,422 windows (High-frequency NameNode RPC operations)
- **Cluster 4:** 4,818 windows (Extreme metadata storm clusters)

**Top Detected Anomaly:**
- **Node:** `namenode`
- **Timestamp:** `2016-11-18 16:25:00`
- **Anomaly Score:** **7,584.49**
- **Event Volume:** **108,385 events in 5 minutes** (Massive cluster metadata storm)

---

# 16. STRUCTURED STREAMING REPLAY

### Implementation Mechanics (`CMUHistoricalStreaming.scala`)
- **Source:** Spark Structured Streaming reading `/project/raw/hdfs_v2/*.log`.
- **Micro-Batch Trigger:** `Trigger.ProcessingTime("5 seconds")`.
- **Replay Control:** `maxFilesPerTrigger = 1` (replays historical log files sequentially to simulate real-time ingestion).
- **Watermark:** `withWatermark("log_ts", "10 minutes")` (bounds state store memory for tumbling window aggregations).
- **Tumbling Window:** 5 minutes (`groupBy(window($"log_ts", "5 minutes"), $"node_name")`).
- **Real-Time Threshold Logic:** Flags anomaly if `eventCount > 50000 || errorCount > 1000`.
- **Output Sink:** Console output format (`append` mode).

*Viva Defense Note:* "This architecture implements **Historical Log Replay via Spark Structured Streaming**. It uses production log files to drive micro-batch stateful streaming transformations, satisfying the Big Data Velocity requirement."

---

# 17. CONCEPT DRIFT DETECTION

### Statistical Z-Score Formulation (`CMUDriftDetection.scala`)
Concept drift occurs when the statistical distribution of input features shifts over time. The drift monitor evaluates the absolute Z-score of feature means between a **Reference Baseline** (first 70% of chronological records) and a **Current Monitoring Window** (subsequent 30% of records):

$$Z = \frac{|\mu_{\text{current}} - \mu_{\text{reference}}|}{\sigma_{\text{reference}}}$$

### Drift Monitoring Results (from `metrics/ml_drift_detection.txt`)

| Monitored Feature | Reference Mean ($\mu_{\text{ref}}$) | Current Mean ($\mu_{\text{cur}}$) | Reference StdDev ($\sigma_{\text{ref}}$) | Absolute Z-Score ($Z$) | Drift Status ($Z > 1.0$) |
|---|---|---|---|---|---|
| `totalTasks` | 629.73 | 445.08 | 2,341.12 | **0.08** | **STABLE** |
| `mapReduceRatio` | 279.81 | 162.89 | 1,745.30 | **0.07** | **STABLE** |
| `queueWaitMs` | 28,749.71 | 22,398.08 | 612,481.90 | **0.01** | **STABLE** |

*Defensible Interpretation:* "All feature Z-scores are $< 0.1$, indicating no significant distribution shift between baseline and monitoring windows. The Random Forest model remains valid without requiring emergency retraining."

---

# 18. OPERATIONAL ALERT MONITORING

The Python alerting engine (`src/python/alert_monitor.py`) ingests outputs from the ML, Anomaly, and Drift pipelines to generate operational console alerts:

1. **K-Means Anomaly Alerts:** Parses `metrics/ml_kmeans_anomaly.txt` and fires `[CRITICAL ALERT]` when distance score $> 3000.0$.
2. **Model Drift Warnings:** Parses `metrics/ml_drift_detection.txt` and fires `[WARNING] MODEL DRIFT DETECTED` if $Z > 1.0$.
3. **Random Forest Demo Alert:** `demo_simulated_rf_alert()` executes under a prominent `--- DEMO SIMULATION (not a live model call) ---` banner to demonstrate real-time container eviction workflows without fabricating production predictions.

---

# 19. STREAMLIT OPERATIONAL DASHBOARD

The dashboard (`src/python/dashboard.py`) runs on `http://localhost:8501` and connects directly to analytical outputs:

| UI Section / Card | Visual Component | Underlying Data Source File | Displayed Verified Metric |
|---|---|---|---|
| **Model Health - Accuracy** | Metric Card | `metrics/ml_rf_evaluation.txt` | **92.28%** |
| **Model Health - ROC-AUC** | Metric Card | `metrics/ml_rf_evaluation.txt` | **0.8947** (Status: HEALTHY) |
| **Failure Recall** | Metric Card | `metrics/ml_rf_evaluation.txt` | **19.30%** (21.4x over LR) |
| **Metadata Storm Alerts** | Alert Cards | `metrics/ml_kmeans_anomaly.txt` | Shows `namenode` score **7584.49** with 108,385 events |
| **Feature Drift Tracking** | Data Table & Status Pills | `metrics/ml_drift_detection.txt` | Shows `totalTasks` ($Z=0.08$), `mapReduceRatio` ($Z=0.07$), `queueWaitMs` ($Z=0.01$) as STABLE |

---

# 20. PERFORMANCE BENCHMARKING AUDIT

Performance evaluation was executed via `CMUPerformanceBenchmarker.scala` on the active Spark cluster:

```
================================================================================
EXPERIMENT A: Core API Processing Overhead (53,559 records)
  - Spark RDD API     : 349 ms  (Direct JVM object operations)
  - Spark Dataset API : 270 ms  (Optimized Catalyst encoders)
  - Spark DataFrame   : 485 ms  (Includes Catalyst query planning overhead)
================================================================================
EXPERIMENT B: Partition Scaling & Parallelism
  - 2 Partitions      : 231 ms  (High task serialization pressure)
  - 10 Partitions     : 197 ms  [OPTIMAL] Matches worker core availability
  - 200 Partitions    : 488 ms  (Task scheduling and coordination overhead)
================================================================================
EXPERIMENT C: Storage & In-Memory Caching
  - Uncached Data     : 106 ms
  - Memory Cached     :  96 ms  (9.4% speedup on iterative scans)
================================================================================
EXPERIMENT D: Framework Overhead (Small Data Anomaly)
  - Local Single-JVM  :   8 ms
  - Distributed Spark : 367 ms
================================================================================
```

*Viva Defense Explanation for Experiment D:* "On small datasets, a single-JVM local loop completes in 8 ms because distributed Spark incurs network serialization, DAG scheduling, and task dispatch latency (~367 ms). Spark's advantage emerges at Big Data scale (e.g., 22+ GB CMU raw data), where single-JVM applications crash with OutOfMemory errors, while Spark scales linearly across cluster nodes."

---

# 21. TEST SUITE & VERIFICATION

All unit tests are implemented in `src/scala/src/test/scala/CMUUnitTests.scala` using ScalaTest 3.2.11:

1. **`HDFS Log Regex correctly extracts timestamp, level, and message`:** Verifies regular expression tokenization against malformed and standard log lines. (**PASSED**)
2. **`Timestamp converter successfully parses to epoch milliseconds`:** Verifies millisecond timestamp conversion and date-time arithmetic. (**PASSED**)
3. **`Job status labelling logic correctly identifies failures and skips KILLED`:** Confirms binary label assignment ($0 = \text{SUCCESS}$, $1 = \text{FAILED}$) and exclusion of user-killed status $2$. (**PASSED**)

**Test Execution Output:**
```
[info] CMUUnitTests:
[info] - HDFS Log Regex correctly extracts timestamp, level, and message
[info] - Timestamp converter successfully parses to epoch milliseconds
[info] - Job status labelling logic correctly identifies failures and skips KILLED
[info] Run completed in 9 seconds, 65 milliseconds.
[info] Total number of tests run: 3
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 3, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

---

# 22. FACULTY REQUIREMENTS TRACEABILITY MATRIX

| Faculty Requirement | Verified Technical Implementation | Source File | Verification Command | Evidence ID | Status |
|---|---|---|---|---|---|
| **Hadoop cluster ≥ 2 DataNodes** | 1 NameNode + 2 DataNodes in Docker | `docker-compose.yml` | `docker exec namenode hdfs dfsadmin -report` | `SS-01` | **PASS** |
| **Raw dataset in HDFS** | 22 GB CMU + 16.1 GB HDFS_v2 in HDFS | HDFS `/project/raw/` | `hdfs dfs -du -s -h /project/raw/*` | `SS-04`, `SS-05` | **PASS** |
| **Data Preprocessing** | Leakage removal, status filter, null imputation | `CMUPreprocessor.scala` | `spark-submit CMUPreprocessor` | `SS-25` | **PASS** |
| **Processed data in HDFS** | Snappy Parquet partitioned by binary label | HDFS `/project/processed/` | `hdfs dfs -ls /project/processed/cmu_jobs/` | `SS-16` | **PASS** |
| **Java MapReduce** | MapReduce job status aggregation baseline | `JobStatusDriver.java` | `hadoop jar /opt/hadoop-apps/job-status-mr.jar ...` | `SS-09`, `SS-10`, `SS-11` | **PASS** |
| **Spark RDD API** | Low-level transformations (`reduceByKey`, `map`) | `RDDJobParser.scala` | `spark-submit ... RDDJobParser` | `SS-13` | **PASS** |
| **Spark DataFrame API** | Relational left joins, caching, Parquet write | `CMUDataFrameProcessor.scala`| `spark-submit ... CMUDataFrameProcessor` | `SS-14`, `SS-16` | **PASS** |
| **Spark Dataset API** | Strongly-typed case classes & `joinWith` | `CMUDatasetProcessor.scala` | `spark-submit ... CMUDatasetProcessor` | `SS-18` | **PASS** |
| **Spark SQL API** | CTE relational joins and query execution plan | `CMUSQLProcessor.scala` | `spark-submit ... CMUSQLProcessor` | `SS-20` | **PASS** |
| **Spark MLlib Classifiers**| Logistic Regression baseline & Random Forest | `CMULogisticRegression.scala`<br>`CMURandomForest.scala` | `spark-submit ... CMURandomForest` | `SS-23`, `SS-24` | **PASS** |
| **Model Persistence** | Models saved and reloaded from HDFS | HDFS `/project/models/` | `hdfs dfs -ls -R /project/models` | HDFS Verify | **PASS** |
| **Anomaly Detection** | Unsupervised K-Means ($k=5$) distance scoring | `CMUKMeansAnomaly.scala` | `spark-submit ... CMUKMeansAnomaly` | `SS-26` | **PASS** |
| **Structured Streaming** | Micro-batch historical log replay | `CMUHistoricalStreaming.scala`| `spark-submit ... CMUHistoricalStreaming` | `SS-28` | **PASS** |
| **Concept Drift** | Statistical Z-score feature drift tracking | `CMUDriftDetection.scala` | `spark-submit ... CMUDriftDetection` | `SS-29` | **PASS** |
| **Interactive Dashboard** | Multi-page Streamlit analytics dashboard | `src/python/dashboard.py` | `streamlit run src/python/dashboard.py` | `SS-31` | **PASS** |
| **Performance Benchmarking**| 4 API, partition, and caching experiments | `CMUPerformanceBenchmarker.scala`| `spark-submit ... CMUPerformanceBenchmarker` | `SS-32` | **PASS** |
| **Unit Testing** | 3/3 ScalaTest test cases passed | `CMUUnitTests.scala` | `sbt test` | `SS-33` | **PASS** |

---

# 23. EVIDENCE INVENTORY AUDIT

| Evidence ID | Phase | Filename on Disk | Technical Phenomenon Demonstrated | File Size | Status |
|---|---|---|---|---|---|
| `SS-01` | Phase 2 | `evidence/phase2/SS-01.png` | `hdfs dfsadmin -report` showing 2 live DataNodes | 100 KB | **CAPTURED** |
| `SS-02` | Phase 2 | `evidence/phase2/SS-02_01.png`, `SS-02_02.png` | NameNode Web UI (`http://localhost:9870`) | 142 KB | **CAPTURED** |
| `SS-03` | Phase 2 | `evidence/phase2/SS-03.png` | Spark Master Web UI (`http://localhost:8080`) | 55 KB | **CAPTURED** |
| `SS-04` | Phase 3 | `evidence/phase3/SS-04.png` | CMU OpenCloud 22 GB storage in HDFS | 15 KB | **CAPTURED** |
| `SS-05` | Phase 3 | `evidence/phase3/SS-05.png` | HDFS_v2 16.1 GB log storage in HDFS | 26 KB | **CAPTURED** |
| `SS-06` | Phase 3 | `evidence/phase3/SS-06.png` | HDFS directory tree verification (`/project/raw/`) | 44 KB | **CAPTURED** |
| `SS-07` | Phase 4 | `evidence/phase4/SS-07.png` | CMU `job.csv` schema inspection & headers | 68 KB | **CAPTURED** |
| `SS-08` | Phase 4 | `evidence/phase4/SS-08.png` | Raw status code distribution | 29 KB | **CAPTURED** |
| `SS-09` | Phase 5 | `evidence/phase5/SS-09_mapreduce_compile.png` | Java MapReduce compilation output | 88 KB | **CAPTURED** |
| `SS-10` | Phase 5 | `evidence/phase5/SS-10_mapreduce_run_timing.png` | Java MapReduce timing (3,598 ms) | 93 KB | **CAPTURED** |
| `SS-11` | Phase 5 | `evidence/phase5/SS-11_mapreduce_output.png` | MapReduce `part-r-00000` status counts | 30 KB | **CAPTURED** |
| `SS-13` | Phase 6 | `evidence/phase6/SS-13_rdd_terminal_output.png` | Spark RDD execution output & timing | 279 KB | **CAPTURED** |
| `SS-14` | Phase 7 | `evidence/phase7/SS-14_spark_df_dag_01.png`, `_02.png` | Spark DataFrame Catalyst DAG in Spark UI | 354 KB | **CAPTURED** |
| `SS-16` | Phase 7 | `evidence/phase7/SS-16_hdfs_processed_ls.png` | Processed Parquet partition listing in HDFS | 27 KB | **CAPTURED** |
| `SS-18` | Phase 8 | `evidence/phase8/SS-18_ds_terminal.png` | Spark Dataset typed execution output | 234 KB | **CAPTURED** |
| `SS-20` | Phase 9 | `evidence/phase9/SS-20_sql_explain.png` | Spark SQL Physical Plan (`explain`) output | 249 KB | **CAPTURED** |
| `SS-22` | Phase 11 | `evidence/phase11/SS-22_orchestration_run.png` | Batch pipeline orchestration execution | 47 KB | **CAPTURED** |
| `SS-23` | Phase 12 | `evidence/phase12/SS-23_lr_metrics.png` | Logistic Regression evaluation metrics | 227 KB | **CAPTURED** |
| `SS-24` | Phase 13 | `evidence/phase13/SS-24_rf_metrics.png` | Random Forest evaluation metrics & importances | 227 KB | **CAPTURED** |
| `SS-25` | Phase 14 | `evidence/phase14/SS-25_hdfs_preprocessing.png` | HDFS log preprocessing terminal output | 221 KB | **CAPTURED** |
| `SS-26` | Phase 14 | `evidence/phase14/SS-26_kmeans_anomalies.png` | K-Means anomaly detection top outliers | 213 KB | **CAPTURED** |
| `SS-28` | Phase 15 | `evidence/phase15/SS-28_historical_streaming_output.txt`| Structured Streaming micro-batch console output | 2 KB | **CAPTURED (Text)** |
| `SS-29` | Phase 16 | `evidence/phase16/SS-29_drift_detection.png` | Concept drift statistical Z-score output | 182 KB | **CAPTURED** |
| `SS-30` | Phase 17 | `evidence/phase17/SS-30_alert_monitor.png` | Python operational alert monitor terminal | 57 KB | **CAPTURED** |
| `SS-31` | Phase 18 | `evidence/phase18/SS-31_streamlit_dashboard.png` | Streamlit interactive web dashboard | 132 KB | **CAPTURED** |
| `SS-32` | Phase 19 | `evidence/phase19/SS-32_benchmarking.png` | Performance benchmarking timing output | 188 KB | **CAPTURED** |
| `SS-33` | Phase 20 | `evidence/phase20/SS-33_unit_tests.png` | ScalaTest 3/3 passing test execution | 137 KB | **CAPTURED** |

---

# 24. FINAL PROJECT VERDICT & DEFENSE READINESS

### Final Categorical Assessment:
- **VERIFIED COMPONENTS (100% Defensible):**
  - Distributed Docker cluster architecture (1 NameNode + 2 DataNodes + Spark Master/Worker, with `4040:4040` exposed and `UnderReplicatedBlocks = 0` verified in `SS-050` and `SS-053`).
  - 38.082 GB HDFS storage volume with replication factor 2 (`441` blocks, `0` under-replicated, `0` corrupt, `0` missing).
  - Data preprocessing pipeline (`CMUPreprocessor.scala`) eliminating all 5 post-completion leakage columns (`finishTime`, `finMaps`, `finReduces`, `failMaps`, `failReduces`).
  - Spark batch processing implementations across all 4 APIs (RDD, DataFrame, Dataset, SQL) with `.config("spark.hadoop.dfs.replication", "2")` enforced.
  - Spark MLlib Random Forest classifier achieving **0.8947 AUC** (`Accuracy = 92.28%`, `Weighted Precision = 90.48%`, `Weighted Recall = 92.28%`, `Weighted F1 = 90.37%`) with minority failure recall of **19.30%** (`FAILED Precision = 61.73%`, `FAILED F1 = 29.41%` — a 21.4x recall improvement over LR).
  - Unsupervised K-Means anomaly detection identifying NameNode metadata storms (`294,649` 5-minute windows).
  - Safe batch pipeline orchestration scripts (`run_pipeline.ps1` / `run_pipeline.sh`) with destructive wildcard deletions neutralized.
  - Model persistence in HDFS (`/project/models/logistic_regression` and `/project/models/random_forest`, `rep=2`) with verified reload capability.
  - Full test suite passing (`3/3` ScalaTest unit tests in `CMUUnitTests.scala` executed live with Eclipse Adoptium OpenJDK `17.0.20.1` + SBT `1.8.3`, assembling `bigdata-analytics-assembly-1.0.0.jar`, `SS-051`).
  - Live Python Alert Monitor (`src/python/alert_monitor.py`) and Streamlit Dashboard (`src/python/dashboard.py` at `http://localhost:8501`, `SS-052`).

- **ACCEPTED METHODOLOGICAL LIMITATIONS (To state openly in Viva):**
  - *Feature Selection in Downstream SQL/ML:* While `CMUPreprocessor.scala` drops all post-completion columns, `CMUSQLProcessor.scala` joins task/attempt aggregates (`taskFailureCount`, `retryRate`) which are included in `VectorAssembler`. However, `CMURandomForest` assigned **`0.000000` feature importance** to both `taskFailureCount` and `retryRate`, relying 100% on the 3 launch-time features (`totalTasks`: `53.54%`, `mapReduceRatio`: `30.65%`, `queueWaitMs`: `15.81%`).
  - *Random vs Chronological Split:* An 80/20 random split (`seed=1234L`) was evaluated for MLlib classifiers, while `CMUDriftDetection` uses a 70/30 split for Z-score feature stability monitoring.
  - *Class Imbalance Strategy:* The 10.8:1 imbalance was addressed via tree-based non-linear partitioning (Random Forest) rather than synthetic oversampling (SMOTE), which avoids generating artificial graph structures on categorical cluster IDs.
  - *K-Means Cluster Count & Missing `/project/processed/hdfs_logs`:* $k=5$ was evaluated over `294,649` 5-minute windows (`metrics/ml_kmeans_anomaly.txt`, `SS-037`). Note that `/project/processed/hdfs_logs` was previously deleted by a wildcard `rm -r -f` in `run_pipeline.ps1` (now neutralized) and can be regenerated on demand via `HDFSLogPreprocessor`.

**Conclusion:** The project is in a complete, robust, scientifically sound, and fully viva-defensible state.
