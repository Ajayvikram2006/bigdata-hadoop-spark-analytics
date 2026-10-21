# FINAL ACCEPTANCE REPORT — 2026-10-21

| Component | Status | One-line evidence |
|---|---|---|
| HDFS and DataNodes | PASS | Safe mode OFF; 2 live DataNodes; 1.97 TB cap / 1.68 TB free; 0 missing, 0 corrupt, 0 under-replicated; all required dirs readable except `processed/hdfs_logs` (see below); ports 9870/8080/8081 = 200, 7077 TCP open. MongoDB up (inspect only; dbs admin/config/local). |
| Spark Master/Worker | PASS (after fix) | Master ALIVE but had **0 workers** ("heartbeat from unregistered worker" after Master restart). `docker restart spark-worker` only → registered, ALIVE, 4 cores / 4 GB, `spark://spark-master:7077`. Earlier KILLED executors = normal app teardown (exitStatus 143/0), not Worker failure. Port 4040 already mapped; no compose change. |
| JDK/SBT and tests | PASS (limited) | Scoped JDK 17.0.20.1 (global Java 25 untouched); `sbt -batch test assembly` exit 0; CMUUnitTests 3 passed/0 failed; JAR 113,113 B, 2026-10-21 07:33, contains Benchmarker + RDDJobParser. Caveat: only 3 tests (timestamp/preprocessing logic) — not full coverage. |
| RDD/project Spark job | PARTIAL | `RDDJobParser` NOT run: writes fixed paths `/project/batch_results/rdd_status_counts` and `rdd_queue_stats` via `saveAsTextFile` (no override; would fail/collide with existing results). Fallback: read-only `CMUPerformanceBenchmarker` ran on the cluster, 28 jobs SUCCEEDED, 38 stages, 13 SQL executions. |
| Live project DAG | PASS (run) / screenshots PENDING (user) | App `app-20261021092840-0005` CMUPerformanceBenchmarker-Phase19 ran on spark://spark-master:7077; 4040 = 200 with 28 succeeded jobs, executor 0 on worker; `--hold-ui 600` verified in JAR. Log: `evidence/dag_benchmark_run.log`. Screenshots must be taken by user while UI is held (~15:11 local), else rerun command below. |
| MapReduce status counts | PASS | Independent CSV-aware count over 20 `job.csv` = 55,252 rows: status 0=49,026, 1=4,534, 2=1,666, 6=26. HDFS output: SUCCESS 49026, FAILED 4534, KILLED 1666, UNKNOWN_6 26 — exact match. Mapper maps raw codes (0/1/2, else `UNKNOWN_n`); no code fix needed. The `{0:41986,1:11573,4:1693}` figure sums to the same 55,252 but is not found in the repo and is NOT the raw `status` column — treat as a different/undocumented field; do not cite as MapReduce output. |
| HDFS log preprocessing | PASS | Approved run exited 0: 58,095,613 records parsed, Parquet at `/project/processed/hdfs_logs` (`_SUCCESS`, 1.5 GB, partitioned by level), fsck HEALTHY, replication 2.0, 0 missing/corrupt. Log: `evidence/hdfs_log_preprocess_run.log`. |
| Dashboard and alert monitor | PASS | `dashboard.py` rewritten (backup `archive/backups/src/python/dashboard.py.bak_20261021_final`): metric cards, RF bar chart, K-Means score chart, drift chart, banner "Reads saved metrics, not live data.", labelled DEMO alert. Port 8501 = 200; rendered values verified in browser (acc 0.9228, AUC 0.8947, FAILED recall 0.1930, drift z 0.08/0.07/0.01). `alert_monitor.py` exit 0. |
| Script safety | PASS | No `/project/processed/*` or `/project/models/*` deletes in either script. Only `-rm -r -f` targets `rdd_status_counts` and `rdd_queue_stats` (note: re-running the pipeline deletes those existing RDD results). `.ps1:89` is a read-only `du`. Scripts not executed. |

## Notes / honest caveats
- RF FAILED-class recall is only 19.3% (high accuracy driven by class imbalance); dashboard states this.
- Benchmark timings are sub-second on a 53,559-row dataset; "local vs distributed" numbers are illustrative only.

## Demo URLs
HDFS 9870 · Spark Master 8080 · Worker 8081 · Spark app UI 4040 (only while an app runs) · Dashboard 8501

## Relaunch DAG (if the UI hold has expired)
```
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.benchmarking.CMUPerformanceBenchmarker --executor-memory 1g --total-executor-cores 2 /tmp/bigdata-stage.jar --hold-ui 600
```
Pages: /jobs/, /stages/ (job → DAG Visualization), /SQL/, /executors/

## HDFS log preprocessing — executed after approval
```
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.preprocessing.HDFSLogPreprocessor --executor-memory 2g --total-executor-cores 4 --conf spark.hadoop.dfs.replication=2 /tmp/bigdata-stage.jar
```
Reads 16.1 GB; may take a long time on a 4-core/4 GB worker. Optional `--input-glob "hadoop-hdfs-datanode-mesos-0*.log"` limits input for a smaller trial (but would write a partial `hdfs_logs`).

## Manual steps remaining
1. Screenshots of 4040 Jobs/Stages-DAG/SQL/Executors (relaunch if expired) and dashboard 8501.
2. Approve (or decline) the HDFS log command above.
3. Decide how to describe the `{0:41986,1:11573,4:1693}` figure in the viva (not raw status).


---
# FACULTY DEMO READINESS UPDATE — 2026-10-21 (15:25 local)

| Component | Status | Actual evidence | Remaining action |
|---|---|---|---|
| HDFS NameNode | PASS | UI 9870 = 200; RPC 9000 open; safe mode OFF; 761 blocks | none |
| DataNodes | PASS | datanode1 + datanode2 healthy, 2 live / 0 dead | none |
| HDFS replication and block health | PASS | dfs.replication=2 (getconf). fsck HEALTHY on all 7 dirs, avg replication 2.0, every file shows repl 2 (raw/cmu 140 files, raw/hdfs_v2 31, cmu_jobs 3, hdfs_logs 291, batch_results 26, mapreduce_output 2, models 10). NameNode: under-replicated 0, corrupt 0, missing 0, pending 0. New Spark writes: confirmed for hdfs_logs (written by job with spark.hadoop.dfs.replication=2 and cluster default 2) | none |
| Spark Master | PASS | ALIVE, 7077 open, 8080 = 200 | none |
| Spark Worker | PASS | Registered, ALIVE, 4 cores / 4 GB, 8081 = 200; restart count 0, OOMKilled false | none |
| Spark KILLED-status diagnosis | PASS (not a fault) | Every KILLED in Worker log is preceded by "Asked to kill executor" at the moment the driver app finished and stopped (exit 143 = SIGTERM, 0 = clean). Master "unknown executor" WARNs are the benign post-removal race. No heartbeat timeout, crash or OOM. KILLED label = executor of a completed application | explain in viva |
| Project benchmark and exit code | PASS | app-20261021094445-0007 (CMUPerformanceBenchmarker-Phase19): EXIT_CODE=0 in `evidence/dag_benchmark_run2.log`; 28 jobs SUCCEEDED, 38 stages (28 complete + 10 skipped), 13 SQL executions, 0 failed tasks | none |
| Live Spark DAG evidence | PARTIAL (screenshots pending, user) | UI 4040 was live and API-verified during the hold; hold has now ended (port 4040 closes with the app). I cannot capture screenshots | relaunch and screenshot (command below) |
| Dashboard | PASS | 8501 = 200; browser DOM check: banner, acc 0.9228, AUC 0.8947, drift z=0.08, DEMO alert present, no exceptions | screenshot |
| HDFS log preprocessing | PASS | fsck HEALTHY, 291 files, 290 blocks, repl 2.0, `_SUCCESS` present, ~1.5 GB. 58,095,613 record count taken from the job log; NOT recounted (avoided a new Spark job) | optional |
| Overall faculty demonstration readiness | PASS pending screenshots | Infra healthy, no faults found this round; no restarts or changes made | take screenshots |

Notes: K-Means rerun (exit 0, 294,649 windows, same top anomaly) is not numerically identical to the saved metrics (K-Means is stochastic across runs/partitioning; top score 4945 vs 7584; cluster sizes differ). Saved `metrics/ml_kmeans_anomaly.txt` kept and backed up (`archive/backups/src/python/dashboard.py.bak_20261021_final`); dashboard/alert monitor read it. RDDJobParser stays PARTIAL: hard-coded output paths already hold results; not rerun, nothing deleted.

## Screenshot steps
1. Run (read-only, ~1 min of work then UI held 10 min):
   `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.benchmarking.CMUPerformanceBenchmarker --executor-memory 1g --total-executor-cores 2 /tmp/bigdata-stage.jar --hold-ui 600`
2. http://localhost:4040/jobs/ ; /stages/ → click a stage → DAG Visualization ; /SQL/ → click a query ; /executors/
3. http://localhost:8080 (Master, Running Application) ; http://localhost:8081 ; http://localhost:9870/dfshealth.html#tab-overview and #tab-datanode ; http://localhost:8501

---
# FINAL AUDIT, ORGANIZATION AND RUNBOOK — 2026-10-21 (documentation task)

**Scope:** read-only checks + documentation/organization. **No** build, test, Spark job, MapReduce, ML, streaming, preprocessing, container restart or pipeline script was run in this task. Runtime behaviour was not re-executed after the comment-only source edits (JAR not rebuilt).

## Evidence categories
| Category | Items |
|---|---|
| Verified current state (read-only, this task) | 5 containers up (namenode/datanodes healthy, MongoDB untouched); NameNode: 2 live DataNodes, safe mode OFF, under-replicated 0 / corrupt 0 / missing 0; `dfs.replication=2`; `/project/processed/hdfs_logs` exists, fsck HEALTHY, avg replication 2.0, `_SUCCESS` repl 2; Spark Master ALIVE, 1 worker ALIVE (4 cores, 4 GB), 0 active apps; ports 9870/8080/8081/8501 = HTTP 200, 9000/7077 TCP open, **4040 closed (no driver running — expected)** |
| Fresh execution on 2026-10-21 (earlier in the day, before this task) | sbt test+assembly (3/3, exit 0); benchmark `app-20261021094445-0007` exit 0 (28 jobs, 38 stages [28 complete/10 skipped], 13 SQL, 0 failed tasks); HDFS log preprocessing (58,095,613 records); K-Means rerun (exit 0, 294,649 windows); MapReduce counts re-derived from raw CSVs; dashboard rewrite + alert monitor exit 0 |
| Earlier historical runs only (not freshly rerun) | RDDJobParser, DataFrame, Dataset, SQL, Logistic Regression, Random Forest, Drift, Historical Streaming, MapReduce job execution (output dated 2026-10-21 01:51) |
| Commands documented for future use | everything in `docs/RUNBOOK.md` (startup, shutdown, per-component submit commands, DAG sequence) |

## Findings of this audit
- **MapReduce driver deletes its output directory if it exists** (`JobStatusDriver.java`, `fs.delete`). Running it with default arguments would erase the verified result; the runbook prescribes a new output path. Code left unchanged. `scripts/run_pipeline.*` call it with defaults — another reason not to run them.
- **K-Means rerun differs numerically from saved metrics** (top score 4945 vs 7584; cluster sizes differ; same 294,649 windows, same top anomaly). Saved `metrics/ml_kmeans_anomaly.txt` kept; backup in `archive/backups/metrics/`.
- **RDDJobParser rerun limitation:** hard-coded output paths already occupied (`saveAsTextFile` fails if present) — remains PARTIAL, not rerun.
- **Spark UI 4040 is temporary:** exists only while a driver runs; relaunch command in runbook section F. Screenshots must be taken by the user.
- `CMUDataFrameProcessor` and `CMUPreprocessor` both overwrite `/project/processed/cmu_jobs`; `CMUSQLProcessor` overwrites the input of all ML/benchmark jobs.

## Deliverables
`docs/RUNBOOK.md` (authoritative) · `docs/PROJECT_MAP.md` · `docs/ORGANIZATION_CHANGE_LOG.md` · viva comments in 17 source files · this report · `evidence-register.md`.
