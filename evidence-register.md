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
| SS-33 | Phase 20 | evidence/phase20/SS-33_unit_tests.png | SBT ScalaTest passing results (3/3 passed) | COMPLETE | 137 KB |
| SS-34 | Phase 21 | evidence/phase21/SS-34_cluster_teardown.png | Final `docker-compose down` execution | PENDING (Teardown phase) | — |
