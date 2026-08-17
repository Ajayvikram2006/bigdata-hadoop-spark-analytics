# PROJECT AUDIT REPORT (POST-REMEDIATION FINAL)
## Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Course: 23AID302 — Big Data Analytics
## Last Audit & Remediation Date: 2026-08-17
## Methodology: Forensic File-by-File Inspection & Live Cluster Execution

---

## EXECUTIVE SUMMARY & FINAL HEALTH RATING

### FINAL OVERALL PROJECT HEALTH: 🟢 GREEN

**Justification:**
All critical, medium, and minor findings identified during the initial audit have been systematically remediated and verified through live execution against the 5-container Hadoop/Spark cluster. 
1. The build system compiles all 13 Scala files from a clean state and passes all unit tests (3/3).
2. The batch pipeline runs end-to-end without errors.
3. Machine learning models compute complete evaluation metrics (Precision, Recall, F1, Confusion Matrix, AUC), persist to HDFS (`/project/models/`), and verify independent reload.
4. Historical Structured Streaming evidence has been captured and documented.
5. All documentation (`requirements-traceability.md`, `evidence-register.md`, `DOCKER_VIVA_RUNBOOK.md`) accurately reflects the live environment.

---

## REMEDIATION HISTORY & VERIFICATION MATRIX

| Finding ID | Component | Original Issue | Remediation Action Taken | Live Verification Result | Status |
|---|---|---|---|---|---|
| **CRIT-01** | Docker | Docker Desktop was stopped | Cluster started (`docker-compose up -d`) | `docker ps` confirms 5 containers; `hdfs dfsadmin -report` confirms 2 live DataNodes (80 GB used). | **RESOLVED** |
| **CRIT-02** | Streaming | `SS-28` streaming screenshot missing | Executed `CMUHistoricalStreaming` for 4 micro-batches | Captured terminal output to `evidence/phase15/SS-28_historical_streaming_output.txt`. | **RESOLVED** |
| **CRIT-03** | MLlib | Models not persisted to HDFS | Added `.save()` and reload tests to `CMULogisticRegression` and `CMURandomForest` | `hdfs dfs -ls -R /project/models` confirms both models saved with data, metadata, and snappy parquet. | **RESOLVED** |
| **MED-01** | Pipeline | `run_pipeline.sh` referenced nonexistent `resourcemanager` | Corrected container name to `namenode`, JAR to `/opt/hadoop-apps/job-status-mr.jar`, class to `JobStatusDriver` | `powershell -File scripts\run_pipeline.ps1` completed end-to-end with exit code 0. | **RESOLVED** |
| **MED-02** | Build | `build.sbt` missing `rdd`, `dataframe`, `dataset` dirs | Added missing source dirs and fixed test path to `src/test/scala` | Clean `sbt clean compile test assembly` compiled 13 sources, passed 3/3 tests, assembled 107 KB JAR. | **RESOLVED** |
| **MED-03** | ML Metrics | Precision/Recall/F1/Confusion Matrix missing | Added `MulticlassClassificationEvaluator` and per-class failure metrics | Evaluated: RF achieved **AUC=0.8947, Minority Recall=19.30%, Precision=61.73%** (vs LR Recall=0.90%). | **RESOLVED** |
| **MED-04** | ML Tuning | LR baseline had AUC=0.5000 | Tuned `regParam=0.01` (pure L2) and `maxIter=100` | Re-execution lifted LR ROC-AUC to **0.6871**. | **RESOLVED** |
| **MIN-01** | Traceability | `requirements-traceability.md` had all items as PENDING | Rewrote traceability matrix with real status, file links, and evidence IDs | Traceability matrix fully updated and synchronized. | **RESOLVED** |
| **MIN-02** | Evidence | `evidence-register.md` contained duplicate row blocks | De-duplicated table and matched exact on-disk files and sizes | Clean evidence table verified. | **RESOLVED** |
| **MIN-03** | Alerting | `alert_monitor.py` had unlabeled simulated alert | Renamed to `demo_simulated_rf_alert()` with explicit DEMO SIMULATION banner | Tested and verified in Python execution. | **RESOLVED** |

---

## TECHNICAL SYSTEM ARCHITECTURE (VERIFIED)

```
Big Data Storage Layer:
  - HDFS Cluster: 1 NameNode (port 9870/9000) + 2 DataNodes
  - Volume: 38.082 GB total (22.025 GB CMU + 16.057 GB HDFS_v2)
  - Replication Factor: 2 (configured in hadoop.env)
  - Block Size: 128 MB

Compute & Analytics Layer:
  - MapReduce: Java 8 MapReduce running in LOCAL mode inside namenode (3.6s execution)
  - Spark Cluster: Spark 3.2.1 Standalone Master (port 8080/7077) + 1 Worker (4 Cores, 4 GB RAM)
  - Processing APIs: Spark RDD (Phase 6), DataFrame (Phase 7), Dataset (Phase 8), Spark SQL (Phase 9)
  - Spark MLlib: Logistic Regression (Phase 12), Random Forest (Phase 13), K-Means Clustering (Phase 14)
  - Structured Streaming: Historical HDFS log replay windowed across 5-minute tumbling intervals (Phase 15)

Monitoring & UI Layer:
  - Drift Detection: Statistical Z-score monitoring across temporal features (Phase 16)
  - Alert Monitor: Python console alerting system (Phase 17)
  - Dashboard: Streamlit interactive UI on port 8501 (Phase 18)
```

---

## MACHINE LEARNING PERFORMANCE SUMMARY

| Metric | Logistic Regression (Baseline) | Random Forest (Primary Model) | Analysis / Key Finding |
|---|---|---|---|
| **Accuracy** | 91.53% | **92.28%** | High baseline accuracy reflects the 91.53% majority SUCCESS class. |
| **Weighted Precision** | 0.8630 | **0.9048** | RF achieves higher precision across all classes. |
| **Weighted Recall** | 0.9153 | **0.9228** | RF maintains high overall recall. |
| **Weighted F1-Score** | 0.8776 | **0.9037** | Significant F1 improvement with RF. |
| **ROC-AUC** | 0.6871 | **0.8947** | RF shows strong non-linear discriminative power. |
| **Minority (FAILED) Precision**| 26.67% | **61.73%** | 61.73% of RF failure predictions are true job failures. |
| **Minority (FAILED) Recall** | 0.90% (8 / 886) | **19.30% (171 / 886)** | **RF catches 21.4x more actual job failures** than LR. |
| **Top Predictive Features** | `totalTasks`, `mapReduceRatio` | `totalTasks` (53.5%), `mapReduceRatio` (30.6%), `queueWaitMs` (15.8%) | Workload scale and task skew are the strongest failure drivers. |
| **Model Persistence** | `/project/models/logistic_regression` | `/project/models/random_forest` | Saved to HDFS and reload-verified. |

---

## FINAL FACULTY COMPLIANCE MATRIX

| Faculty Requirement | Verified Implementation | Evidence ID | Result |
|---|---|---|---|
| Hadoop cluster with ≥ 2 DataNodes | `docker-compose.yml` (1 NameNode + 2 DataNodes) | `SS-01` | **PASS** |
| Raw dataset stored in HDFS | 22 GB CMU (`/project/raw/cmu`) + 16.1 GB HDFS_v2 (`/project/raw/hdfs_v2`) | `SS-04`, `SS-05` | **PASS** |
| Data Preprocessing & Cleaning | Dropped leakage columns, filtered invalid statuses, handled nulls | `SS-16`, `SS-25` | **PASS** |
| Java MapReduce implementation | `JobStatusMapper`, `JobStatusReducer`, `JobStatusDriver` | `SS-09`, `SS-10`, `SS-11` | **PASS** |
| Spark RDD API | `RDDJobParser.scala` using raw transformations | `SS-13` | **PASS** |
| Spark DataFrame API | `CMUDataFrameProcessor.scala` with left joins & caching | `SS-14`, `SS-16` | **PASS** |
| Spark Dataset (Typed) API | `CMUDatasetProcessor.scala` with case classes & `joinWith` | `SS-18` | **PASS** |
| Spark SQL processing | `CMUSQLProcessor.scala` with CTE execution plan | `SS-20` | **PASS** |
| Spark MLlib classification | Logistic Regression baseline + Random Forest primary classifier | `SS-23`, `SS-24` | **PASS** |
| Model Persistence in HDFS | Trained models persisted and reloaded from `/project/models/` | HDFS Verification | **PASS** |
| Unsupervised Anomaly Detection | K-Means clustering (k=5) over 5-minute node log windows | `SS-26` | **PASS** |
| Structured Streaming | Historical log replay via Spark Structured Streaming | `SS-28` | **PASS** |
| Concept Drift Monitoring | Statistical Z-score feature distribution tracking | `SS-29` | **PASS** |
| Interactive Dashboard | Streamlit Web UI (`dashboard.py`) | `SS-31` | **PASS** |
| End-to-End Performance Benchmarks | API comparison, partition tuning, caching evaluation | `SS-32` | **PASS** |
| Unit Testing | 3 ScalaTest unit tests (Regex, Converter, Labeling) — 3/3 passed | `SS-33` | **PASS** |
