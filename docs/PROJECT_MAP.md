# Project Map — pipeline flow and folder structure

Run guide: [`RUNBOOK.md`](RUNBOOK.md) · Results: [`reports/FINAL_ACCEPTANCE_REPORT.md`](reports/FINAL_ACCEPTANCE_REPORT.md) · Evidence index: [`reports/evidence-register.md`](reports/evidence-register.md) · What moved where: [`ORGANIZATION_CHANGE_LOG.md`](ORGANIZATION_CHANGE_LOG.md)

## 1. The flow (read the folders top to bottom)

```
 DATA                    INGEST / CLEAN           BATCH ANALYTICS               ML & ANOMALY                SERVE
 ----                    --------------           ---------------               ------------                -----
 data/raw/cmu_opencloud  src/scala/               src/mapreduce  (MapReduce)    src/scala/                  src/python/
 data/raw/hdfs_v2   -->  01_preprocessing    -->  src/scala/02_batch_processing 03_machine_learning   -->    dashboard.py
   (copied into HDFS      CMUPreprocessor          rdd -> dataframe ->            LogReg, RandomForest        alert_monitor.py
    /project/raw)         HDFSLogPreprocessor      dataset -> sql                 04_anomaly_and_drift        reads metrics/
                                                   (sql_output = ML input)         K-Means, Drift
                                                                                 05_streaming (replay)
                                                   06_benchmark_dag  --> Spark UI :4040 (Jobs / Stages-DAG / SQL / Executors)
```
Stage numbers match execution order. Everything runs on the Docker cluster in `docker/` (NameNode, 2 DataNodes, Spark Master + Worker); HDFS replication factor is 2.

## 2. Source code by stage

| Stage | Folder | Files | Spark/Hadoop entry point |
|---|---|---|---|
| 0 Batch baseline | `src/mapreduce/` | `JobStatusMapper.java`, `JobStatusReducer.java`, `JobStatusDriver.java` | `edu.project1.mapreduce.JobStatusDriver` |
| 1 Preprocessing | `src/scala/01_preprocessing/` | `CMUPreprocessor.scala`, `HDFSLogPreprocessor.scala` | `edu.project1.preprocessing.*` |
| 2a RDD | `src/scala/02_batch_processing/rdd/` | `RDDJobParser.scala` | `edu.project1.rdd.RDDJobParser` |
| 2b DataFrame | `src/scala/02_batch_processing/dataframe/` | `CMUDataFrameProcessor.scala` | `edu.project1.dataframe.CMUDataFrameProcessor` |
| 2c Dataset | `src/scala/02_batch_processing/dataset/` | `CMUDatasetProcessor.scala` | `edu.project1.dataset.CMUDatasetProcessor` |
| 2d Spark SQL | `src/scala/02_batch_processing/sql/` | `CMUSQLProcessor.scala` | `edu.project1.sql.CMUSQLProcessor` |
| 3 Machine learning | `src/scala/03_machine_learning/` | `CMULogisticRegression.scala`, `CMURandomForest.scala` | `edu.project1.ml.*` |
| 4 Anomaly and drift | `src/scala/04_anomaly_and_drift/` | `CMUKMeansAnomaly.scala`, `CMUDriftDetection.scala` | `edu.project1.ml.*` |
| 5 Streaming | `src/scala/05_streaming/` | `CMUHistoricalStreaming.scala` (+ `CMUStreamingJob.scala` socket demo) | `edu.project1.streaming.*` |
| 6 Benchmark / DAG | `src/scala/06_benchmark_dag/` | `CMUPerformanceBenchmarker.scala` | `edu.project1.benchmarking.CMUPerformanceBenchmarker` (`--hold-ui N`) |
| 7 Serving | `src/python/` | `dashboard.py`, `alert_monitor.py` | `streamlit run` / `python` |
| Build | `src/scala/` | `build.sbt`, `project/`, tests in `src/scala/src/test/scala/CMUUnitTests.scala` | sbt `test assembly` |

Package names (`edu.project1.*`) are independent of the folder names, so the class names used in `spark-submit --class` did not change.

## 3. Top-level folders

| Folder | Purpose |
|---|---|
| `src/` | All source code (above). `src/scala/target/` holds build output incl. the assembly JAR |
| `docker/` | `docker-compose.yml`, `hadoop.env` — the cluster definition |
| `scripts/` | MapReduce build helpers and the full-pipeline scripts (`run_pipeline.*`, do not run casually) |
| `data/raw/` | Raw datasets on the host: `cmu_opencloud/` (22 GB), `hdfs_v2/`, `hdfs_v2_extracted/` — immutable, git-ignored |
| `data/local_outputs/` | Empty local placeholders (`models`, `predictions`, `results`, `alerts`, `streaming_output`); real outputs live in HDFS `/project/...` |
| `metrics/` | Saved evaluation/timing results (read by the dashboard and alert monitor) |
| `evidence/` | Run logs, per-phase screenshots (`phase0`–`phase20`) and `final_screenshots/` with its index |
| `docs/` | `RUNBOOK.md`, `PROJECT_MAP.md`, `ORGANIZATION_CHANGE_LOG.md`; `reports/` (acceptance report, evidence register, requirements traceability, audit); `history/` (dev diary, command log); `archive/` (superseded runbooks) |
| `archive/` | `backups/` (every `.bak*`, mirrored), `misc/` (scratch + legacy empty folders) |

## 4. HDFS layout (`/project`)
`raw/cmu`, `raw/hdfs_v2` (inputs) → `processed/cmu_jobs`, `processed/hdfs_logs` → `batch_results/{sql_output,dataset_output,rdd_status_counts,rdd_queue_stats}` → `models/{logistic_regression,random_forest}` · `mapreduce_output/job_status_counts` · `streaming_output`.
