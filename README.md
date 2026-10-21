# Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform

**Course:** 23AID302 — Big Data Analytics
**First Review:** October 5, 2026

> **Start here:** run/verify everything with [`docs/RUNBOOK.md`](docs/RUNBOOK.md); find any source file in [`docs/PROJECT_MAP.md`](docs/PROJECT_MAP.md); current results in [`docs/reports/FINAL_ACCEPTANCE_REPORT.md`](docs/reports/FINAL_ACCEPTANCE_REPORT.md).

## Repository Structure (follows the pipeline flow)

```
Project_1/
|-- README.md                  this file
|-- docker/                    cluster definition (NameNode, 2 DataNodes, Spark Master/Worker)
|-- data/raw/                  immutable source datasets (CMU OpenCloud, HDFS_v2)
|-- src/
|   |-- mapreduce/             Java MapReduce baseline
|   |-- scala/
|   |   |-- 01_preprocessing/      raw -> clean Parquet
|   |   |-- 02_batch_processing/   rdd / dataframe / dataset / sql
|   |   |-- 03_machine_learning/   Logistic Regression, Random Forest
|   |   |-- 04_anomaly_and_drift/  K-Means, drift detection
|   |   |-- 05_streaming/          historical Structured Streaming replay
|   |   `-- 06_benchmark_dag/      performance benchmark + Spark DAG demo
|   `-- python/                dashboard + alert monitor
|-- metrics/                   saved results read by the dashboard
|-- scripts/                   MapReduce build + pipeline scripts
|-- evidence/                  run logs and screenshots
|-- docs/                      RUNBOOK, PROJECT_MAP, reports/, history/, archive/
`-- archive/                   backups and legacy files (not used at runtime)
```
Details: [`docs/PROJECT_MAP.md`](docs/PROJECT_MAP.md).

## Project Overview

A production-grade Big Data analytics system built on Apache Hadoop and Apache Spark,
processing 38+ GB of real workload data across two analytical branches:

**Branch A — CMU OpenCloud (22 GB)**
Supervised job failure prediction using Spark MLlib (Logistic Regression + Random Forest)
on 55,252 Hadoop job records spanning May 2010 to December 2011.

**Branch B — HDFS_v2 (16 GB)**
Unsupervised anomaly detection on 31 HDFS operational log files via K-Means clustering,
Structured Streaming historical replay, and statistical drift detection.

## Technology Stack

| Layer | Technology |
|---|---|
| Cluster | Docker — 1 NameNode + 2 DataNodes + Spark Master + Worker |
| Storage | HDFS (Apache Hadoop 3.x) |
| Batch Processing | Apache Spark 3.x (Scala) + Java MapReduce baseline |
| ML | Spark MLlib — Logistic Regression, Random Forest, K-Means |
| Streaming | Spark Structured Streaming (historical log replay) |
| Dashboard | Python / Streamlit |
| Version Control | Git |

## Datasets

| Dataset | Source | Size | Type |
|---|---|---|---|
| CMU OpenCloud Hadoop Workload | Carnegie Mellon University | 22.025 GB | Structured CSV |
| HDFS_v2 Node Logs | CUHK Production Cluster | 16.057 GB | Semi-structured logs |

## Key Technical Facts

- Total analytical volume: ~38.08 GB
- Job records for ML: 53,560 (after KILLED exclusion)
- Failure rate: 8.47% — class weighting applied in all models
- Temporal split: Train 2010-05→2011-06 | Val 2011-07→2011-09 | Test 2011-10→2011-12
- HDFS_v2 anomaly detection: unsupervised only (no labels in dataset)

## Documentation

| File | Purpose |
|---|---|
| docs/history/development.md | Engineering diary — complete implementation history |
| docs/reports/requirements-traceability.md | Faculty requirement → implementation → evidence mapping |
| docs/reports/evidence-register.md | Screenshot inventory (SS-01 through SS-37) |
| docs/history/commands_history.log | All commands run with timestamps and results |

## Evidence

All screenshots with SS-XX IDs are in evidence/ organized by phase.
