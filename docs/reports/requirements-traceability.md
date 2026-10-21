# requirements-traceability.md
## Project: Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Rule: Map every faculty requirement to implementation and evidence. Updated: 2026-10-21 (Final Gap-Closure Edition)

| Requirement | Phase | Implementation File | Evidence ID | Status |
|---|---|---|---|---|
| Hadoop cluster ≥ 2 DataNodes | 2 | docker/docker-compose.yml — datanode1 + datanode2 | SS-01, SS-002, SS-003, SS-050 | **COMPLETE** |
| NameNode WebUI accessible | 2 | docker/docker-compose.yml port 9870 (`UnderReplicatedBlocks=0`) | SS-02, SS-002, SS-050, SS-053 | **COMPLETE** |
| Spark Master WebUI accessible | 2 | docker/docker-compose.yml port 8080 (`Alive Workers: 1`) | SS-03, SS-046, SS-049 | **COMPLETE** |
| Raw CMU dataset in HDFS | 3 | /project/raw/cmu/ (22.025 GB, 20 months, 140 files = 120 CSVs + 20 .gitkeep) | SS-04, SS-06, SS-006, SS-013 | **COMPLETE** |
| Raw HDFS_v2 dataset in HDFS | 3 | /project/raw/hdfs_v2/ (16.057 GB, 31 log files) | SS-05, SS-06, SS-007 | **COMPLETE** |
| HDFS directory structure verified | 3 | hdfs dfs -ls -R /project/raw/ | SS-06, SS-005 | **COMPLETE** |
| Data schema inspection | 4 | job.csv header: 12 columns confirmed | SS-07 | **COMPLETE** |
| Target class distribution | 4 | SUCCESS=49026, FAILED=4534, KILLED=1666, UNKNOWN_6=26 | SS-08, SS-029 | **COMPLETE** |
| Java MapReduce compilation | 5 | src/mapreduce/JobStatusMapper/Reducer/Driver.java → job-status-mr.jar | SS-09, SS-027 | **COMPLETE** |
| MapReduce execution + timing | 5 | hadoop jar /opt/hadoop-apps/job-status-mr.jar edu.project1.mapreduce.JobStatusDriver | SS-10, SS-028 | **COMPLETE** |
| MapReduce output in HDFS | 5 | /project/mapreduce_output/job_status_counts/part-r-00000 | SS-11, SS-010, SS-029 | **COMPLETE** |
| Spark RDD processing | 6 | src/scala/02_batch_processing/rdd/RDDJobParser.scala — textFile/filter/map/flatMap/reduceByKey | SS-13, SS-030 | **COMPLETE** |
| Spark RDD DAG visualization | 6 | Spark Application UI localhost:4040 (`--hold-ui 120` supported) | SS-049 (Master App UI); Live 4040 via container `spark-submit` | **PARTIAL (Master App UI captured; live 4040 command ready)** |
| Spark DataFrame processing | 7 | src/scala/02_batch_processing/dataframe/CMUDataFrameProcessor.scala | SS-14, SS-16, SS-031, SS-032 | **COMPLETE** |
| DataFrame schema evidence | 7 | printSchema() output in DataFrame execution log | SS-031, SS-032 | **COMPLETE** |
| Processed Parquet in HDFS | 7 | /project/processed/cmu_jobs/ (partitioned label=0/1, rep=2) | SS-16, SS-008, SS-014, SS-015 | **COMPLETE** |
| Spark Dataset (typed API) | 8 | src/scala/02_batch_processing/dataset/CMUDatasetProcessor.scala — case classes + joinWith | SS-18, SS-034 | **COMPLETE** |
| Spark SQL processing | 9 | src/scala/02_batch_processing/sql/CMUSQLProcessor.scala — CTE query + explain(true) | SS-20, SS-016, SS-035 | **COMPLETE** |
| Scala preprocessing code | 9 | src/scala/01_preprocessing/CMUPreprocessor.scala | SS-25, SS-008 | **COMPLETE** |
| HDFS log preprocessing | 14.1 | src/scala/01_preprocessing/HDFSLogPreprocessor.scala | SS-25, SS-036 | **PARTIAL (Historical output + metrics verified; `/project/processed/hdfs_logs` regeneration requires container `spark-submit`)** |
| Feature engineering | 9/12 | queueWaitMs, totalTasks, mapReduceRatio | metrics files, SS-041 | **COMPLETE** |
| Leakage prevention | 9 | Dropped 5 post-completion columns in `CMUPreprocessor`; documented `taskFailureCount`/`retryRate` `0.000000` RF importance | docs/PROJECT_AUDIT.md | **COMPLETE** |
| Data cleaning | 9 | Filter KILLED/UNKNOWN_6, remove negative queueWait (`53,560 -> 53,559`), fill nulls | code + HDFS outputs verified | **COMPLETE** |
| Spark MLlib — Logistic Regression | 12 | src/scala/03_machine_learning/CMULogisticRegression.scala (`Accuracy=0.9153`, `AUC=0.6871`, `Weighted F1=0.8776`) | SS-041 | **COMPLETE** |
| Spark MLlib — Random Forest | 13 | src/scala/03_machine_learning/CMURandomForest.scala (`Accuracy=0.9228`, `AUC=0.8947`, `Weighted F1=0.9037`) | SS-041 | **COMPLETE** |
| Precision/Recall/F1 metrics | 12/13 | Full multiclass & minority `FAILED` metrics (`FAILED Recall=19.30%`, `Precision=61.73%`, `F1=29.41%`) | SS-041 | **COMPLETE** |
| Confusion Matrix | 12/13 | Verified in `metrics/ml_lr_evaluation.txt` and `metrics/ml_rf_evaluation.txt` | SS-041 | **COMPLETE** |
| Model persistence to HDFS | 12/13 | Persisted in HDFS `/project/models/logistic_regression` & `/project/models/random_forest` (`rep=2`) | SS-011, SS-017 | **COMPLETE** |
| K-Means anomaly detection | 14.2 | src/scala/04_anomaly_and_drift/CMUKMeansAnomaly.scala — k=5, 5-min windows (294,649 windows) | SS-26, SS-037 | **COMPLETE** |
| Structured Streaming | 15 | src/scala/05_streaming/CMUHistoricalStreaming.scala — HDFS file source | SS-28 text + SS-042 | **COMPLETE** |
| Data drift detection | 16 | src/scala/04_anomaly_and_drift/CMUDriftDetection.scala — Z-score comparison | SS-29, SS-038 | **COMPLETE** |
| Python alert monitor | 17 | src/python/alert_monitor.py | SS-30, SS-039, SS-043 | **COMPLETE** |
| Streamlit dashboard | 18 | src/python/dashboard.py (`http://localhost:8501`) | SS-31, SS-018, SS-052 | **COMPLETE** |
| Spark API performance comparison | 19 | src/scala/06_benchmark_dag/CMUPerformanceBenchmarker.scala | SS-32, SS-040 | **COMPLETE** |
| Partition tuning evidence | 19 | Experiment B: 2 vs 200 vs 10 partitions | SS-32, SS-040 | **COMPLETE** |
| Caching strategy evidence | 19 | Experiment C: None vs Memory vs Disk | SS-32, SS-040 | **COMPLETE** |
| Unit tests | 20 | src/scala/src/test/scala/CMUUnitTests.scala — 3/3 ScalaTest tests passed with OpenJDK 17.0.20.1 | SS-051 | **COMPLETE** |
| Data Volume (Big Data) | 3 | 22.025 GB CMU + 16.057 GB HDFS_v2 = 38.082 GB total | SS-04, SS-05, SS-023 | **COMPLETE** |
| Data Variety | 3 | Structured CSV (CMU) + Unstructured text logs (HDFS_v2) | Multiple | **COMPLETE** |
| Data Velocity | 15 | Structured Streaming replay of HDFS_v2 logs | SS-042 | **COMPLETE** |
| Data Veracity | 9 | Leakage removal, null handling, negative queueWait filtering | Code + metrics | **COMPLETE** |
| Pipeline orchestration script | All | scripts/run_pipeline.ps1 & scripts/run_pipeline.sh (wildcard `rm -r -f` neutralized; `rep=2` enforced) | Code verified | **COMPLETE** |
| 2 DataNodes confirmed active | 2 | Live NameNode UI & JMX (`NumLiveDataNodes: 2`) | SS-002, SS-003, SS-050 | **COMPLETE** |
| Processed data back in HDFS | 9 | /project/processed/cmu_jobs/ Parquet partitions (`rep=2`) | SS-008, SS-014, SS-015 | **COMPLETE** |
| HDFS replication = 2 (0 under-rep) | 2 | WebHDFS `SETREPLICATION=2` + Spark `.config("spark.hadoop.dfs.replication", "2")` | SS-050, SS-053 | **COMPLETE** |
| Cluster teardown evidence | 21 | `docker-compose stop` (safe stop preserving volumes) | Runbook Workflow 37 | **COMPLETE** |

## NOTES
- **KILLED class excluded** from ML training — only binary SUCCESS/FAILED used (53,559 records)
- **Class imbalance**: 91.53% SUCCESS / 8.47% FAILED (10.8:1 ratio) — no resampling; model uses class weighting approach documentation is in docs/PROJECT_AUDIT.md
- **MapReduce mode**: LOCAL (not YARN) — MapReduce computation runs inside namenode container
- **Random Forest AUC = 0.8947** is the primary model (LR AUC = 0.5000 intentionally presented as failed baseline comparison)
- **build.sbt fix**: rdd/, dataframe/, dataset/ added to unmanagedSourceDirectories (remediation 2026-08-17)
- **SS-28**: Structured Streaming screenshot — requires cluster restart and re-run
