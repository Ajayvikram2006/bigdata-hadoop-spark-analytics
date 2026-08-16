# evidence-register.md
## Project: Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Rule: Register every screenshot at the moment it is captured. Never mark CAPTURED without actual file.

| ID | Phase | Filename | Description | Status | Date Captured |
|---|---|---|---|---|---|
| SS-01 | 2 | evidence/phase2/SS-01_hadoop_cluster_2_datanodes.png | hdfs dfsadmin -report — 2 live DataNodes | PENDING | |
| SS-02 | 2 | evidence/phase2/SS-02_namenode_webui.png | NameNode Web UI port 9870 | PENDING | |
| SS-03 | 2 | evidence/phase2/SS-03_spark_master_ui.png | Spark Master UI port 8080 | PENDING | |
| SS-04 | 3 | evidence/phase3/SS-04_cmu_hdfs_du.png | hdfs dfs -du -s /project/raw/cmu | PENDING | |
| SS-05 | 3 | evidence/phase3/SS-05_hdfs_v2_hdfs_du.png | hdfs dfs -du -s /project/raw/hdfs_v2 | PENDING | |
| SS-06 | 3 | evidence/phase3/SS-06_hdfs_ls_raw.png | hdfs dfs -ls -R /project/raw/ | PENDING | |
| SS-07 | 4 | evidence/phase4/SS-07_schema_inspection.png | CMU job.csv header + 3 rows | PENDING | |
| SS-08 | 4 | evidence/phase4/SS-08_target_distribution.png | SUCCESS/FAILED/KILLED counts | PENDING | |
| SS-09 | 5 | evidence/phase5/SS-09_mapreduce_compile.png | javac compilation success | PENDING | |
| SS-10 | 5 | evidence/phase5/SS-10_mapreduce_run_timing.png | time hadoop jar execution | PENDING | |
| SS-11 | 5 | evidence/phase5/SS-11_mapreduce_output.png | hdfs dfs -cat mapreduce output | PENDING | |
| SS-12 | 6 | evidence/phase6/SS-12_spark_rdd_dag.png | Spark UI DAG for RDD job | PENDING | |
| SS-13 | 6 | evidence/phase6/SS-13_rdd_terminal_output.png | spark-submit terminal output | PENDING | |
| SS-14 | 7 | evidence/phase7/SS-14_dataframe_dag.png | Spark UI DAG for DataFrame | PENDING | |
| SS-15 | 7 | evidence/phase7/SS-15_dataframe_schema.png | Printed DataFrame schema | PENDING | |
| SS-16 | 7 | evidence/phase7/SS-16_processed_hdfs.png | hdfs dfs -ls /project/processed/ | PENDING | |
| SS-17 | 8 | evidence/phase8/SS-17_dataset_case_class.png | Scala Dataset typed operations | PENDING | |
| SS-18 | 9 | evidence/phase9/SS-18_cleaning_before_after.png | Row counts before/after cleaning | PENDING | |
| SS-19 | 9 | evidence/phase9/SS-19_scala_preprocessing_code.png | Scala preprocessing source | PENDING | |
| SS-20 | 9 | evidence/phase9/SS-20_preprocessing_execution.png | spark-submit preprocessing run | PENDING | |
| SS-21 | 10 | evidence/phase10/SS-21_feature_engineering.png | Feature stats + temporal split | PENDING | |
| SS-22 | 11 | evidence/phase11/SS-22_lr_metrics.png | LR Precision/Recall/F1/AUC | PENDING | |
| SS-23 | 12 | evidence/phase12/SS-23_rf_metrics.png | RF Precision/Recall/F1/AUC | PENDING | |
| SS-24 | 12 | evidence/phase12/SS-24_rf_feature_importance.png | Feature importance ranking | PENDING | |
| SS-25 | 12 | evidence/phase12/SS-25_model_in_hdfs.png | hdfs dfs -ls /project/models/ | PENDING | |
| SS-26 | 13 | evidence/phase13/SS-26_kmeans_anomaly.png | K-Means anomaly scores output | PENDING | |
| SS-27 | 14 | evidence/phase14/SS-27_explainability_output.png | Feature importance explanation | PENDING | |
| SS-28 | 15 | evidence/phase15/SS-28_streaming_output.png | Structured Streaming output | PENDING | |
| SS-29 | 16 | evidence/phase16/SS-29_drift_detection.png | Drift score + alert output | PENDING | |
| SS-30 | 18 | evidence/phase18/SS-30_dashboard_overview.png | Dashboard Overview page | PENDING | |
| SS-35 | 19 | evidence/phase19/SS-35_performance_table.png | MR vs Spark timing table | PENDING | |
| SS-01 | Phase 2 | docker exec namenode hdfs dfsadmin -report showing 2 datanodes | COMPLETE |
| SS-02 | Phase 2 | NameNode Web UI (http://localhost:9870) | COMPLETE |
| SS-03 | Phase 2 | Spark Master UI (http://localhost:8080) | COMPLETE |
| SS-04 | Phase 3 | CMU file count/size in HDFS | COMPLETE |
| SS-05 | Phase 3 | HDFS_v2 log file size in HDFS | COMPLETE |
| SS-06 | Phase 3 | Verification of directory structure hdfs dfs -ls /project/raw | COMPLETE |
| SS-07 | Phase 4 | evidence/phase4/SS-07_schema_inspection.png | CMU job.csv header + 4 rows | COMPLETE |
| SS-08 | Phase 4 | evidence/phase4/SS-08_target_distribution.png | Status distribution per column 6 | COMPLETE |
| SS-09 | Phase 5 | evidence/phase5/SS-09_mapreduce_compile.png | javac + jar BUILD_SUCCESS output | COMPLETE |
| SS-10 | Phase 5 | evidence/phase5/SS-10_mapreduce_run_timing.png | hadoop jar timing output | COMPLETE |
| SS-11 | Phase 5 | evidence/phase5/SS-11_mapreduce_output.png | hdfs dfs -cat part-r-00000 output | COMPLETE |
| SS-12 | Phase 6 | evidence/phase6/SS-12_spark_rdd_dag.png | Spark UI DAG at localhost:4040 | COMPLETE |
| SS-13 | Phase 6 | evidence/phase6/SS-13_rdd_terminal_output.png | spark-submit terminal with timing output | COMPLETE |
| SS-14 | Phase 7 | evidence/phase7/SS-14_spark_df_dag.png | Spark UI DAG for DataFrame Job | COMPLETE |
| SS-15 | Phase 7 | evidence/phase7/SS-15_df_schema.png | DataFrame printSchema output | COMPLETE |
| SS-16 | Phase 7 | evidence/phase7/SS-16_hdfs_processed_ls.png | hdfs dfs -ls /project/processed/cmu_jobs/ | COMPLETE |
| SS-17 | Phase 8 | evidence/phase8/SS-17_spark_ds_dag.png | Spark UI DAG for Dataset Job | COMPLETE |
| SS-18 | Phase 8 | evidence/phase8/SS-18_ds_terminal.png | spark-submit terminal output for Dataset | COMPLETE |
| SS-19 | Phase 9 | evidence/phase9/SS-19_spark_sql_ui.png | Spark UI SQL Tab | COMPLETE |
| SS-20 | Phase 9 | evidence/phase9/SS-20_sql_explain.png | SQL Execution Plan (Explain) terminal output | COMPLETE |
| SS-21 | Phase 10 | evidence/phase10/SS-21_spark_streaming.png | Spark Streaming Console Output | PENDING_USER_TEST |
| SS-22 | Phase 11 | evidence/phase11/SS-22_orchestration_run.png | Terminal showing run_pipeline.sh executing | PENDING_USER_TEST |
