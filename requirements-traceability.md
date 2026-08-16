# requirements-traceability.md
## Project: Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Rule: Update after every phase. Map every faculty requirement to implementation and evidence.

| Requirement | Phase | Implementation | Evidence ID | Status |
|---|---|---|---|---|
| Hadoop cluster >= 2 DataNodes | 2 | docker-compose 1NN+2DN | SS-01 | PENDING |
| Raw dataset in HDFS | 3 | /project/raw/cmu/ + /project/raw/hdfs_v2/ | SS-04,SS-05 | PENDING |
| Scala preprocessing code | 9 | CMUPreprocessor.scala | SS-19 | PENDING |
| Preprocessing execution | 9 | spark-submit run | SS-20 | PENDING |
| Processed dataset | 9 | /project/processed/ written | SS-16 | PENDING |
| Processed dataset in HDFS | 9 | hdfs dfs -ls /project/processed/ | SS-16 | PENDING |
| HDFS storage evidence | 3,9 | hdfs dfs -du -s /project/ | SS-04,SS-07 | PENDING |
| MapReduce | 5 | Java MapReduce job status counter | SS-09,SS-10,SS-11 | PENDING |
| Spark RDD | 6 | Scala RDD processing | SS-12,SS-13 | PENDING |
| Spark DataFrame | 7 | Scala DataFrame joins | SS-14,SS-15 | PENDING |
| Spark Dataset | 8 | Scala typed Dataset | SS-17 | PENDING |
| Spark ML / MLlib | 11,12 | LR + RF classifiers | SS-22,SS-23 | PENDING |
| Streaming | 15 | Structured Streaming replay | SS-28 | PENDING |
| Dashboard | 18 | Streamlit multi-page | SS-30 | PENDING |
| Performance evaluation | 19 | MR vs Spark timing table | SS-35 | PENDING |
