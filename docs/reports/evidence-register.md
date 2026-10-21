# evidence-register.md
## Project: Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Rule: Register every screenshot at the moment it is captured. Never mark CAPTURED without actual file.

| ID | Phase | Filename | Description | Status | File Size |
|---|---|---|---|---|---|
| SS-01 | Phase 2 | evidence/phase2/SS-01.png | `hdfs dfsadmin -report` showing 2 live DataNodes | COMPLETE | 100 KB |
| SS-02 | Phase 2 | evidence/phase2/SS-02_01.png, SS-02_02.png | NameNode Web UI (`http://localhost:9870`) | COMPLETE | 142 KB |
| SS-03 | Phase 2 | evidence/phase2/SS-03.png | Spark Master UI (`http://localhost:8080`) | COMPLETE | 55 KB |
| SS-04 | Phase 3 | evidence/phase3/SS-04.png | CMU file count/size in HDFS (`hdfs dfs -du -s /project/raw/cmu`) | COMPLETE | 15 KB |
| SS-05 | Phase 3 | evidence/phase3/SS-05.png | HDFS_v2 log file size in HDFS (`hdfs dfs -du -s /project/raw/hdfs_v2`) | COMPLETE | 26 KB |
| SS-06 | Phase 3 | evidence/phase3/SS-06.png | Directory structure verification (`hdfs dfs -ls /project/raw`) | COMPLETE | 44 KB |
| SS-07 | Phase 4 | evidence/phase4/SS-07.png | CMU `job.csv` header and sample schema rows | COMPLETE | 68 KB |
| SS-08 | Phase 4 | evidence/phase4/SS-08.png | Raw status distribution (SUCCESS / FAILED / KILLED) | COMPLETE | 29 KB |
| SS-09 | Phase 5 | evidence/phase5/SS-09_mapreduce_compile.png | Java MapReduce compilation output | COMPLETE | 88 KB |
| SS-10 | Phase 5 | evidence/phase5/SS-10_mapreduce_run_timing.png | MapReduce job execution and timing output | COMPLETE | 93 KB |
| SS-11 | Phase 5 | evidence/phase5/SS-11_mapreduce_output.png | MapReduce output inspection (`part-r-00000`) | COMPLETE | 30 KB |
| SS-12 | Phase 6 | evidence/phase6/SS-12_spark_rdd_dag.png | Spark UI DAG for RDD job | PENDING (Optional during re-run) | — |
| SS-13 | Phase 6 | evidence/phase6/SS-13_rdd_terminal_output.png | `spark-submit` terminal with timing output for RDD | COMPLETE | 279 KB |
| SS-14 | Phase 7 | evidence/phase7/SS-14_spark_df_dag_01.png, SS-14_spark_df_dag_02.png | Spark UI DAG for DataFrame processing | COMPLETE | 354 KB |
| SS-15 | Phase 7 | evidence/phase7/SS-15_df_schema.png | DataFrame `printSchema()` execution output | PENDING (Captured in job log) | — |
| SS-16 | Phase 7 | evidence/phase7/SS-16_hdfs_processed_ls.png | Processed Parquet in HDFS (`hdfs dfs -ls /project/processed/cmu_jobs/`) | COMPLETE | 27 KB |
| SS-17 | Phase 8 | evidence/phase8/SS-17_spark_ds_dag.png | Spark UI DAG for Dataset job | PENDING (Optional) | — |
| SS-18 | Phase 8 | evidence/phase8/SS-18_ds_terminal.png | `spark-submit` terminal output for Dataset API | COMPLETE | 234 KB |
| SS-19 | Phase 9 | evidence/phase9/SS-19_spark_sql_ui.png | Spark UI SQL Execution Tab | PENDING (Optional) | — |
| SS-20 | Phase 9 | evidence/phase9/SS-20_sql_explain.png | SQL Execution Plan (`explain`) output | COMPLETE | 249 KB |
| SS-21 | Phase 10 | evidence/phase10/SS-21_spark_streaming.png | Netcat Socket Streaming Console Output | PENDING (Demo/Optional) | — |
| SS-22 | Phase 11 | evidence/phase11/SS-22_orchestration_run.png | Pipeline orchestration execution | COMPLETE | 47 KB |
| SS-23 | Phase 12 | evidence/phase12/SS-23_lr_metrics.png | Logistic Regression Evaluation Metrics Terminal Output | COMPLETE | 227 KB |
| SS-24 | Phase 13 | evidence/phase13/SS-24_rf_metrics.png | Random Forest Evaluation Metrics Terminal Output | COMPLETE | 227 KB |
| SS-25 | Phase 14 | evidence/phase14/SS-25_hdfs_preprocessing.png | HDFS log parsing preprocessing output | COMPLETE | 221 KB |
| SS-26 | Phase 14 | evidence/phase14/SS-26_kmeans_anomalies.png | K-Means Anomaly Detection output | COMPLETE | 213 KB |
| SS-28 | Phase 15 | evidence/phase15/SS-28_historical_streaming.png | Structured Streaming Historical Replay Console Alerts | **MANDATORY PENDING** | — |
| SS-29 | Phase 16 | evidence/phase16/SS-29_drift_detection.png | Statistical concept drift detection output | COMPLETE | 182 KB |
| SS-30 | Phase 17 | evidence/phase17/SS-30_alert_monitor.png | Python operational alerts terminal output | COMPLETE | 57 KB |
| SS-31 | Phase 18 | evidence/phase18/SS-31_streamlit_dashboard.png | Streamlit Web UI Dashboard | COMPLETE | 132 KB |
| SS-32 | Phase 19 | evidence/phase19/SS-32_benchmarking.png | Terminal output of wall-clock benchmarking | COMPLETE | 188 KB |
| SS-33 | Phase 20 | evidence/phase20/SS-33_unit_tests.png | Historical SBT attempt from `/` (superseded by `SS-051`) | SUPERSEDED BY SS-051 | 137 KB |
| SS-34 | Phase 21 | evidence/phase21/SS-34_cluster_teardown.png | Final `docker-compose stop` execution | OPTIONAL (Keep cluster running for review) | — |
| SS-045 | Phase R3 | evidence/final_screenshots/01_docker_environment/SS-045_live_all_ports_jmx_and_29_underrep_audit.png | Live TCP port probe (`9870,9000,8080,7077,8081,4040,27017`), NameNode JMX, and `29` under-replicated block audit | COMPLETE | 186 KB |
| SS-046 | Phase R3 | evidence/final_screenshots/04_spark_master_worker/SS-046_spark_master_overview_healthy.png | Live Spark Master UI (`http://localhost:8080`) with `Alive Workers: 1` (`4` cores, `4.0 GiB`) and completed `Spark Pi` | COMPLETE | 83 KB |
| SS-047 | Phase R3 | evidence/final_screenshots/04_spark_master_worker/SS-047_spark_worker_registered_healthy.png | Live Spark Worker UI (`http://localhost:8081`) registered with `spark://spark-master:7077` | COMPLETE | 70 KB |
| SS-048 | Phase R3 | evidence/final_screenshots/04_spark_master_worker/SS-048_spark_worker_executor_stderr_proof.png | Live Worker Executor `0` `stderr` proving `Driver commanded a shutdown -> RECEIVED SIGNAL TERM` | COMPLETE | 62 KB |
| SS-049 | Phase R3 | evidence/final_screenshots/04_spark_master_worker/SS-049_spark_master_completed_app_sparkpi.png | Live Spark Master Application page for `app-20261020174921-0000` (`Spark Pi`, `State: FINISHED`) | COMPLETE | 58 KB |
| SS-050 | Phase R4 | evidence/final_screenshots/02_hdfs_namenode_ui/SS-050_hdfs_namenode_zero_underreplicated.png | Live HDFS NameNode UI (`http://localhost:9870/dfshealth.html#tab-overview`) showing `Number of Under-Replicated Blocks: 0` | COMPLETE | 99 KB |
| SS-051 | Phase R4 | evidence/final_screenshots/17_build_and_tests/SS-051_sbt_test_and_assembly_jdk17.png | Fresh live `sbt -batch test assembly` execution with OpenJDK `17.0.20.1` (`3/3` tests passed, `SBT_EXIT_CODE=0`, `110,013 B` JAR built) | COMPLETE | 148 KB |
| SS-052 | Phase R4 | evidence/final_screenshots/16_streamlit_dashboard/SS-052_streamlit_dashboard_live_verified.png | Fresh live full-page dark-mode capture of Streamlit Dashboard (`http://localhost:8501`) | COMPLETE | 151 KB |
| SS-053 | Phase R4 | evidence/final_screenshots/02_hdfs_namenode_ui/SS-053_zero_underreplicated_blocks_webhdfs_proof.png | Live WebHDFS `SETREPLICATION=2` + NameNode JMX verification (`UnderReplicatedBlocks: 0`, `BlocksTotal: 441`) | COMPLETE | 68 KB |

## Final acceptance run — 2026-10-21
See `docs/reports/FINAL_ACCEPTANCE_REPORT.md`. Spark worker re-registered (restart); tests 3/3 under JDK 17; benchmark app-20261021092840-0005 28 jobs SUCCEEDED (log `evidence/dag_benchmark_run.log`); MapReduce counts re-verified from raw CSVs; dashboard rewritten, 8501 = 200; `alert_monitor.py` exit 0; `/project/processed/hdfs_logs` still missing (approval pending).
- HDFS log preprocessing completed 2026-10-21: 58,095,613 records, /project/processed/hdfs_logs fsck HEALTHY, repl 2.

## Demo readiness check — 2026-10-21 15:25
Benchmark app-20261021094445-0007 exit 0 (28 jobs/38 stages/13 SQL); HDFS fsck HEALTHY all /project dirs, repl 2, 0 under-replicated; Spark KILLED = normal executor teardown at app end (no fault); dashboard DOM-verified; K-Means rerun differs numerically, saved metrics preserved. See docs/reports/FINAL_ACCEPTANCE_REPORT.md.

## Final documentation/organization audit — 2026-10-21
Read-only re-check of HDFS (repl 2, 0 bad blocks), Spark (ALIVE, 1 worker), ports (4040 closed = no driver) and dashboard. Created `docs/RUNBOOK.md`, `docs/PROJECT_MAP.md`, `docs/ORGANIZATION_CHANGE_LOG.md`; moved all `*.bak*` to `archive/backups/` and superseded runbooks to `docs/archive/`; added comment-only viva notes to 17 source files. Nothing re-executed; JAR not rebuilt. Note: `JobStatusDriver` deletes an existing output directory — see report.
