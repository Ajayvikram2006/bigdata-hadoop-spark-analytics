# Authoritative Runbook — Big Data Analytics Project

Project root: `D:\BigData\Project_1` · Last audited: 2026-10-21 · Supersedes `docs/archive/RUNBOOK_COMPLETE.md` and `docs/archive/DOCKER_VIVA_RUNBOOK.md`.

**Honesty note.** Commands in this file were derived from the repository (compose file, `build.sbt`, source entry points, scripts) and from earlier evidence. During the final documentation audit **nothing was executed except read-only checks**. Each component states whether its evidence is a *fresh run on 2026-10-21* or an *earlier run*.

**Risk labels:** `READ-ONLY` safe inspection · `START/STOP` changes service state · `BUILDS` recompiles artifacts · `WRITES OUTPUT` creates or overwrites data (inspect destination first) · `LONG-RUNNING` heavy CPU/RAM/time.

---

## 0. Architecture in 60 seconds

| Container | Role | Host ports |
|---|---|---|
| `namenode` | HDFS metadata, block map, replication control | 9870 (web), 9000 (RPC) |
| `datanode1`, `datanode2` | Store HDFS blocks (replication factor 2 = one copy on each) | none published (9864 internal) |
| `spark-master` | Spark standalone Master; also where `spark-submit` is run (driver, client mode) | 8080 (web), 7077 (RPC), 4040 (driver UI) |
| `spark-worker` | Runs executors (4 cores, 4 GB) | 8081 (web) |
| `mongodb` | Separate container, **not in the compose file, not used by this runbook** | 27017 |

The host folder `D:\BigData\Project_1` is mounted as `/host-project` in the namenode, datanodes, master and worker (see `docker/docker-compose.yml`).

## 1. Ports and how to test them

| Port | Type | Proves | Test (PowerShell) |
|---|---|---|---|
| 9870 | NameNode web UI + JMX/WebHDFS | HTTP only proves the UI is up | `(Invoke-WebRequest -UseBasicParsing http://localhost:9870).StatusCode` |
| 9000 | HDFS RPC (not HTTP) | Clients can reach NameNode | `(Test-NetConnection localhost -Port 9000).TcpTestSucceeded` |
| 8080 | Spark Master web UI + `/json/` | Master up; real health = `status: ALIVE` and a worker | `(Invoke-RestMethod http://localhost:8080/json/).status` |
| 7077 | Spark Master RPC (not HTTP) | Applications can register | `(Test-NetConnection localhost -Port 7077).TcpTestSucceeded` |
| 8081 | Spark Worker web UI | Worker process up | `(Invoke-WebRequest -UseBasicParsing http://localhost:8081).StatusCode` |
| 4040 | **Spark driver (application) UI** — temporary | Exists only while a driver JVM runs (see section 7) | `(Invoke-WebRequest -UseBasicParsing http://localhost:4040/jobs/).StatusCode` |
| 8501 | Streamlit dashboard | HTTP 200 only proves the server; check page content too | `(Invoke-WebRequest -UseBasicParsing http://localhost:8501).StatusCode` |
| 27017 | MongoDB (not used here) | — | do not touch |

**HTTP 200 is not a health check.** Service-level health is: HDFS = 2 live DataNodes + 0 missing/corrupt/under-replicated blocks + safe mode OFF; Spark = Master `ALIVE` and ≥1 worker `ALIVE`; dashboard = expected values visible on the page.

Other URLs: `http://localhost:9870/dfshealth.html#tab-overview`, `http://localhost:9870/dfshealth.html#tab-datanode`.

---

## A. Prerequisites and startup  (`START/STOP`)

1. Open PowerShell in the project: Start menu → *Windows PowerShell*, then
```powershell
Set-Location D:\BigData\Project_1
```
2. Check Docker is running (`READ-ONLY`):
```powershell
docker version --format "{{.Server.Version}}"
docker ps -a --format "table {{.Names}}\t{{.Status}}"
```
3. Start only the Hadoop/Spark services (service names from `docker/docker-compose.yml`; MongoDB is not part of this file):
```powershell
docker compose -f docker\docker-compose.yml up -d namenode datanode1 datanode2 spark-master spark-worker
```
(If your Docker only has the old CLI, use `docker-compose` instead of `docker compose`. Not executed during the audit.) Containers use `restart: unless-stopped`, so they normally come back by themselves when Docker Desktop starts.

4. Wait until healthy (`READ-ONLY`; re-run until namenode/datanodes show `(healthy)`, usually 1–2 minutes):
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```
5. Confirm registration (`READ-ONLY`):
```powershell
$n=(Invoke-RestMethod "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem").beans[0]; "live DataNodes: $($n.NumLiveDataNodes)"
$m=Invoke-RestMethod http://localhost:8080/json/; "Master: $($m.status)  workers alive: $($m.aliveworkers)  cores: $($m.cores)"
```
Expected: `2`, `ALIVE`, `1`, `4`.
*Known issue seen on 2026-10-21:* if the Master was restarted but the Worker was not, the Master shows 0 workers and logs "heartbeat from unregistered worker". Fix: `docker restart spark-worker` (only that container).

## B. HDFS verification  (`READ-ONLY`)

```powershell
docker exec namenode hdfs dfsadmin -report
docker exec namenode hdfs dfsadmin -safemode get
docker exec namenode hdfs getconf -confKey dfs.replication
docker exec namenode hdfs dfs -ls /project
docker exec namenode hdfs dfs -ls /project/raw/cmu
docker exec namenode hdfs dfs -ls /project/processed/hdfs_logs
docker exec namenode hdfs dfs -cat /project/mapreduce_output/job_status_counts/part-r-00000
docker exec namenode hdfs dfs -cat /project/batch_results/rdd_status_counts/part-00000
```
Read from `dfsadmin -report`: *Live datanodes (2)*, *Missing blocks: 0*, *Under replicated blocks: 0*, *Blocks with corrupt replicas: 0*, capacity.

### Replication in viva language
- **Replication factor 2** means every HDFS block is stored on two different DataNodes. If one DataNode dies, a full copy still exists on the other, and the NameNode re-creates the missing copy when a node is available.
- Two DataNodes are used because the cluster has exactly two storage nodes; a factor of 3 would leave every block permanently "under-replicated" (HDFS cannot put two copies on one node).
- The **NameNode** keeps the file→block→DataNode map. DataNodes send heartbeats and block reports; if a block has fewer live replicas than its target, the NameNode counts it as *under-replicated* and schedules re-replication.
- The cluster default comes from `HDFS_CONF_dfs_replication=2` in `docker/hadoop.env`. Spark jobs also pass `spark.hadoop.dfs.replication=2` so files written by Spark use 2.
- Per-file replication is metadata, not inferred from node count:
```powershell
docker exec namenode hdfs dfs -stat "%r replicas  %n" /project/processed/hdfs_logs/_SUCCESS
docker exec namenode hdfs fsck /project/processed/hdfs_logs -files -blocks -locations
docker exec namenode hdfs fsck /project -blocks | Select-String "Status|Under-replicated|Average block replication|Corrupt|Missing"
docker exec namenode hdfs dfs -ls /project/raw/hdfs_v2      # 2nd column of each file = replication
```
**Verified read-only on 2026-10-21 (final audit):** default 2; 2 live DataNodes; NameNode under-replicated 0 / corrupt 0 / missing 0; safe mode OFF; `/project/processed/hdfs_logs` exists, `fsck` HEALTHY, average replication 2.0, `_SUCCESS` replication 2.

## C. Spark verification  (`READ-ONLY`)

```powershell
(Invoke-RestMethod http://localhost:8080/json/) | Select-Object status, aliveworkers, cores, coresused, memory
(Invoke-RestMethod http://localhost:8080/json/).workers | Select-Object id, state, cores, memory
(Invoke-RestMethod http://localhost:8080/json/).activeapps    | Select-Object id, name, state
(Invoke-RestMethod http://localhost:8080/json/).completedapps | Select-Object id, name, state
docker logs spark-worker --tail 30
docker logs spark-master --tail 30
```
Expected: Master `ALIVE`; worker `ALIVE`, 4 cores, 4096 MB; `spark://spark-master:7077`.

### What "KILLED" means (viva-ready)
| You see | Meaning | Problem? |
|---|---|---|
| Worker `ALIVE`, registered | Healthy node | No |
| Executor `KILLED` (exit 143 or 0) under a **completed** application | The driver finished and the Master/Worker told the executor to stop ("Asked to kill executor" in worker log). 143 = SIGTERM | **No — normal teardown** |
| Application `FINISHED` | Completed normally | No |
| Executor `FAILED`/`EXITED` with errors, repeated relaunch, OOM | Real executor failure | Yes — read the executor stderr |
| Worker `DEAD`/`UNKNOWN`, or Master shows 0 workers | Worker lost or never re-registered | Yes — restart `spark-worker` only |

Evidence (2026-10-21): every KILLED executor in the worker log was preceded by "Asked to kill executor" at application end; `spark-worker` RestartCount 0, OOMKilled false.

**Port 4040 is application-specific.** The driver UI (Jobs, Stages/DAG, SQL, Executors) lives inside the running `spark-submit` driver and disappears when the application ends. On port 8080 the Master page only lists applications. At the final audit no driver was running, so `4040` was closed — expected, not a fault. To demonstrate it, follow section F.

## D. Java / Scala build and tests  (`BUILDS`)

The project uses the Scala build in `src/scala` (`build.sbt`: Scala 2.12.15, Spark 3.2.1 "provided", sbt-assembly, JAR name `bigdata-analytics-assembly-1.0.0.jar`; `project/build.properties` pins sbt 1.8.3). Sources are in numbered pipeline-stage folders (`01_preprocessing` … `06_benchmark_dag`) registered via `Compile / unmanagedSourceDirectories`; the single test is `src/scala/src/test/scala/CMUUnitTests.scala`.

**Why JDK 17:** sbt 1.8 / Scala 2.12.15 do not work reliably on the machine's global Java 25. A project-scoped Temurin JDK 17.0.20.1 (downloaded earlier via Coursier) is used for this shell only; global `JAVA_HOME` is never changed.

Verified paths on this machine (found on disk, used for the successful 2026-10-21 build):
- JDK 17: `C:\Users\S Ajay Vikram\AppData\Local\Coursier\cache\arc\https\github.com\adoptium\temurin17-binaries\releases\download\jdk-17.0.20.1%252B1\OpenJDK17U-jdk_x64_windows_hotspot_17.0.20.1_1.zip\jdk-17.0.20.1+1`
- sbt launcher: `C:\Users\S Ajay Vikram\AppData\Local\Coursier\cache\arc\https\github.com\sbt\sbt\releases\download\v2.0.8\sbt-2.0.8.zip\sbt\bin\sbt.bat`

```powershell
$jh  = "C:\Users\S Ajay Vikram\AppData\Local\Coursier\cache\arc\https\github.com\adoptium\temurin17-binaries\releases\download\jdk-17.0.20.1%252B1\OpenJDK17U-jdk_x64_windows_hotspot_17.0.20.1_1.zip\jdk-17.0.20.1+1"
$sbt = "C:\Users\S Ajay Vikram\AppData\Local\Coursier\cache\arc\https\github.com\sbt\sbt\releases\download\v2.0.8\sbt-2.0.8.zip\sbt\bin\sbt.bat"
$env:JAVA_HOME = $jh; $env:PATH = "$jh\bin;$env:PATH"      # this PowerShell window only
java -version
Set-Location D:\BigData\Project_1\src\scala
& $sbt -batch test assembly
```
Expected: `Tests: succeeded 3, failed 0`, `target\scala-2.12\bigdata-analytics-assembly-1.0.0.jar` (113,113 bytes at last build). *Evidence: fresh run on 2026-10-21, exit 0. Only 3 unit tests exist (timestamp/preprocessing logic) — not full coverage. Not re-run in the final audit.*

JAR in containers: `/opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` (same MD5 `1aba77af…0117` as the host build) and also visible as `/host-project/src/scala/target/scala-2.12/bigdata-analytics-assembly-1.0.0.jar` through the mount. Check: `docker exec spark-master md5sum /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` vs `Get-FileHash -Algorithm MD5 src\scala\target\scala-2.12\bigdata-analytics-assembly-1.0.0.jar`. After a rebuild: `docker cp src\scala\target\scala-2.12\bigdata-analytics-assembly-1.0.0.jar spark-master:/opt/spark-apps/` (`WRITES OUTPUT`, replaces the staged JAR).

## E. Components

Common Spark submit prefix (inside `spark-master`; all classes are in the one assembly JAR):
```
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.dfs.replication=2 --driver-memory 2g --executor-memory 2g --class <CLASS> /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
(Same flags as `scripts/run_pipeline.sh`, which must **not** be run.) Worker has only 4 GB — run one job at a time.

### E1. Java MapReduce  — `WRITES OUTPUT`
- Source: `src/mapreduce/JobStatusDriver.java`, `JobStatusMapper.java`, `JobStatusReducer.java` (package `edu.project1.mapreduce`).
- Purpose: count jobs per status. Mapper reads CSV column 5 (`status`), maps code 0→SUCCESS, 1→FAILED, 2→KILLED, else `UNKNOWN_<code>`, emits `(label,1)`; reducer (also the combiner) sums.
- Input: `hdfs://namenode:9000/project/raw/cmu/*/job.csv` · Output: `hdfs://namenode:9000/project/mapreduce_output/job_status_counts` (arguments 1 and 2 override).
- Build the JAR (in namenode, already present at `/opt/hadoop-apps/job-status-mr.jar`): `docker exec namenode bash /host-project/scripts/build_mr.sh` (`BUILDS`).
- Run (local mode, as in the pipeline script): `docker exec namenode hadoop jar /opt/hadoop-apps/job-status-mr.jar edu.project1.mapreduce.JobStatusDriver hdfs://namenode:9000/project/raw/cmu/*/job.csv hdfs://namenode:9000/project/mapreduce_output/job_status_counts_rerun`
- **Collision / DANGER:** `JobStatusDriver` itself **deletes the output directory if it exists** (`fs.delete(outPath, true)`, printed as `[WARN] Output path exists — deleting`). Running it with the default arguments would erase the existing `/project/mapreduce_output/job_status_counts` before regenerating it. Always pass a **new** output path as argument 2 (e.g. `/project/mapreduce_output/job_status_counts_rerun`) so the verified result is preserved. The code was not changed in this audit.
- Verify: `docker exec namenode hdfs dfs -cat /project/mapreduce_output/job_status_counts/part-r-00000` → `FAILED 4534 / KILLED 1666 / SUCCESS 49026 / UNKNOWN_6 26`.
- Evidence: output written 2026-10-21 01:51; counts independently re-derived from the 20 raw `job.csv` files on 2026-10-21 (match). Not re-run in the final audit.

### E2. Spark RDD  — `WRITES OUTPUT`  ⚠ KNOWN LIMITATION
- Source: `src/scala/02_batch_processing/rdd/RDDJobParser.scala`, class `edu.project1.rdd.RDDJobParser`.
- Purpose: parse raw CSV lines into RDDs, `filter`/`mapValues`/`reduceByKey` for status counts and queue-wait statistics (RDD = low-level distributed collection; transformations are lazy, `saveAsTextFile` is the action that triggers the job).
- Input: `/project/raw/cmu` · Output (fixed, hard-coded): `/project/batch_results/rdd_status_counts`, `/project/batch_results/rdd_queue_stats` via `saveAsTextFile`.
- **Collision:** both output directories already exist (written 2026-10-21 01:51) and `saveAsTextFile` fails if a directory exists. The paths are constants with no command-line override, so a safe fresh run is **not possible without editing the source** (add an output-path argument) and rebuilding. Never delete the existing results to make it run; a rerun requires a new output destination and your approval.
- Verify existing result: `docker exec namenode hdfs dfs -cat /project/batch_results/rdd_status_counts/part-00000`.
- Evidence: earlier successful run (application `RDDJobParser-Phase6`, FINISHED, in the Master's completed list). **Status PARTIAL — no fresh rerun.**

### E3. Spark DataFrame and preprocessing  — `WRITES OUTPUT` (overwrites)
- `src/scala/02_batch_processing/dataframe/CMUDataFrameProcessor.scala` → `edu.project1.dataframe.CMUDataFrameProcessor`: reads `/project/raw/cmu`, joins job/task data with the DataFrame API (`filter` to binary status SUCCESS=0/FAILED=1, `groupBy(jtid, jobid)` with conditional `sum(when(...))` map/reduce task counts), writes Parquet `.mode("overwrite")` to `/project/processed/cmu_jobs`.
- `src/scala/01_preprocessing/CMUPreprocessor.scala` → `edu.project1.preprocessing.CMUPreprocessor`: same input, **same output path** `/project/processed/cmu_jobs` (overwrite). Run only one of them.
- Collision: both overwrite `/project/processed/cmu_jobs`. Do not run without approval.
- Verify: `docker exec namenode hdfs dfs -ls /project/processed/cmu_jobs`.
- Evidence: earlier runs (Aug 2026, remediation 2026-10-21 01:xx logs in `commands_history.log`); not re-run now.

### E4. Spark Dataset  — `WRITES OUTPUT` (overwrites)
- `src/scala/02_batch_processing/dataset/CMUDatasetProcessor.scala` → `edu.project1.dataset.CMUDatasetProcessor`: typed `case class` Dataset (compile-time type safety, encoders) over `/project/raw/cmu`; writes Parquet overwrite to `/project/batch_results/dataset_output`.
- Verify: `docker exec namenode hdfs dfs -ls /project/batch_results/dataset_output`. Earlier run only.

### E5. Spark SQL  — `WRITES OUTPUT` (overwrites an ML input)
- `src/scala/02_batch_processing/sql/CMUSQLProcessor.scala` → `edu.project1.sql.CMUSQLProcessor`: registers temp views (`jobs`, `tasks`, `attempts`) and uses SQL joins and aggregations over `/project/raw/cmu`; writes `/project/batch_results/sql_output` (Parquet, overwrite).
- **That directory is the input of Logistic Regression, Random Forest, Drift Detection and the Benchmarker** — overwriting it changes everything downstream. Do not rerun casually.
- Verify: `docker exec namenode hdfs dfs -ls /project/batch_results/sql_output`. Earlier run only.

### E6. Logistic Regression and Random Forest  — `WRITES OUTPUT` (overwrites models)
- `src/scala/03_machine_learning/CMULogisticRegression.scala` → `edu.project1.ml.CMULogisticRegression`; `src/scala/03_machine_learning/CMURandomForest.scala` → `edu.project1.ml.CMURandomForest`.
- Input `/project/batch_results/sql_output`. Pipeline: assemble features (`totalTasks, mapReduceRatio, queueWaitMs, taskFailureCount, retryRate`) → 80/20 split → train → evaluate → save model `.write.overwrite().save(...)` to `/project/models/logistic_regression` and `/project/models/random_forest`, then reload to prove persistence.
- Collision: **overwrites the saved models.** Do not rerun without approval.
- Verify: `docker exec namenode hdfs dfs -ls /project/models`; saved metrics `metrics/ml_rf_evaluation.txt` (accuracy 0.9228, AUC 0.8947, FAILED-class recall 0.1930 — low because of class imbalance) and `metrics/ml_lr_evaluation.txt`.
- Evidence: runs of 2026-08-17 (models dated 08-17). Not re-run.

### E7. HDFS log preprocessing and K-Means anomaly detection
- **Preprocessing** — `src/scala/01_preprocessing/HDFSLogPreprocessor.scala` → `edu.project1.preprocessing.HDFSLogPreprocessor`. Input `/project/raw/hdfs_v2/*.log` (16.1 GB), regex-parses `timestamp, level, component, message`, writes Parquet partitioned by `level` to `/project/processed/hdfs_logs` (`overwrite`). Optional `--input-glob`.
  - `LONG-RUNNING` `WRITES OUTPUT`: about 8 minutes observed on 2 cores shared with another app; it **overwrites** the existing output, so a regeneration needs approval. Command for future regeneration only:
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.preprocessing.HDFSLogPreprocessor --executor-memory 2g --total-executor-cores 4 --conf spark.hadoop.dfs.replication=2 /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
```
  - Verify (`READ-ONLY`): `docker exec namenode hdfs fsck /project/processed/hdfs_logs | Select-String "Status|Average block replication"`; `docker exec namenode hdfs dfs -ls /project/processed/hdfs_logs`.
  - Evidence: **fresh run on 2026-10-21**, exit 0, 58,095,613 records, 291 files, ~1.5 GB, replication 2 (`evidence/hdfs_log_preprocess_run.log`). Not re-run in the final audit.
- **K-Means** — `src/scala/04_anomaly_and_drift/CMUKMeansAnomaly.scala` → `edu.project1.ml.CMUKMeansAnomaly`. Reads `/project/processed/hdfs_logs`, aggregates per node into 5-minute windows (`eventCount, errorCount, warningCount, uniqueComponents`), scales features, K-Means K=5, anomaly score = distance to the assigned cluster centre; prints the top 10. **Writes nothing to HDFS or `metrics/`** (metrics are copied by hand), so it is collision-free.
  - `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUKMeansAnomaly --executor-memory 2g --total-executor-cores 2 /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` (under 1 minute observed on 2 cores)
  - Evidence: **fresh run on 2026-10-21**, exit 0, 294,649 windows, same top anomaly (namenode 2016-11-18 16:25, 108,385 events) as the saved metrics. **Scores and cluster sizes differ** (top score 4945 vs saved 7584) because K-Means is initialised randomly and the cluster/partition layout differs between runs. The saved `metrics/ml_kmeans_anomaly.txt` (Aug 16) was deliberately kept; the rerun is in `evidence/kmeans_rerun.log`; the original is also backed up in `archive/backups/metrics/`.

### E8. Historical Structured Streaming replay  — `WRITES OUTPUT`
- `src/scala/05_streaming/CMUHistoricalStreaming.scala` → `edu.project1.streaming.CMUHistoricalStreaming`. **Historical replay, not live ingestion:** it streams the existing log *files* (`maxFilesPerTrigger=1`, 5-second trigger, append mode) so each file becomes one micro-batch, exercising the Structured Streaming engine without a live source. Input `/project/raw/hdfs_v2`; output `/project/streaming_output`; checkpoint `/project/checkpoints/historical_stream`. Optional `--max-seconds N` bounds the run.
- `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.streaming.CMUHistoricalStreaming /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar --max-seconds 60`
- Collision: appends to `/project/streaming_output`; a reused checkpoint resumes rather than restarts. Do not run without approval.
- Verify: `docker exec namenode hdfs dfs -ls /project/streaming_output`; earlier captured micro-batches in `evidence/phase15/SS-28_historical_streaming_output.txt`. Earlier run only.
- Also in the source folder: `CMUStreamingJob.scala` (socket/netcat demo, `local[2]`, usage in its header) — not part of the final evidence.

### E9. Drift detection
- `src/scala/04_anomaly_and_drift/CMUDriftDetection.scala` → `edu.project1.ml.CMUDriftDetection`. Input `/project/batch_results/sql_output`; chronological 70/30 split into *reference* and *current*; compares feature means with a z-score and flags drift above 1.0. Prints results (read-only on HDFS). Saved result `metrics/ml_drift_detection.txt`: all features STABLE (z = 0.08 / 0.07 / 0.01).
- `docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.ml.CMUDriftDetection /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` — earlier run only.

### E10. Performance benchmark and DAG capture  — `READ-ONLY` on HDFS
- `src/scala/06_benchmark_dag/CMUPerformanceBenchmarker.scala` → `edu.project1.benchmarking.CMUPerformanceBenchmarker`. Reads `/project/batch_results/sql_output` (Parquet) and **writes nothing to HDFS**. It compares RDD vs DataFrame vs Dataset, partition counts (2/200/10), no-cache vs memory vs disk cache, and local vs distributed. Every action (`count`, `collect`, …) starts a *job*; every shuffle splits a job into *stages*; the stage graph is the DAG. `--hold-ui <seconds>` (or `-Dspark.ui.holdSeconds`) keeps the driver alive at the end so port 4040 stays open.
- See section F for the command. Evidence: **fresh runs on 2026-10-21**, application `app-20261021094445-0007`: exit 0, 28 jobs SUCCEEDED, 38 stages (28 complete + 10 skipped), 13 SQL executions, 0 failed tasks (`evidence/dag_benchmark_run2.log`). Port 4040 was verified over HTTP/API only; **screenshots must be taken by you**. The `Spark Pi` application is *not* a project application.

### E11. Python alert monitor  — `READ-ONLY`
- `src/python/alert_monitor.py`. Reads `metrics/ml_kmeans_anomaly.txt`, `ml_rf_evaluation.txt`, `ml_drift_detection.txt` and prints colour-coded alerts (anomaly score > 3000 = critical). Does not touch the cluster.
```powershell
Set-Location D:\BigData\Project_1
python src\python\alert_monitor.py; "exit=$LASTEXITCODE"
```
Evidence: run on 2026-10-21 during the acceptance work, exit 0 (`evidence/alert_monitor_run.txt`).

### E12. Streamlit dashboard  — `START/STOP` (local process only)
- `src/python/dashboard.py` (rewritten 2026-10-21; previous version in `archive/backups/src/python/`). Reads the saved files in `metrics/` — **not live data**; the page says so. Shows Random Forest cards and bar chart, K-Means anomaly chart, drift chart and a clearly labelled *DEMO / SIMULATED* alert.
```powershell
Set-Location D:\BigData\Project_1
python -m streamlit run src\python\dashboard.py --server.port 8501 --server.headless true
```
Open `http://localhost:8501`. Check: `(Invoke-WebRequest -UseBasicParsing http://localhost:8501).StatusCode` and verify the page shows *Reads saved metrics, not live data.*, accuracy 0.9228 and AUC 0.8947.
Stop only the dashboard: press `Ctrl+C` in its window, or find its process and stop it by id:
```powershell
Get-NetTCPConnection -LocalPort 8501 -State Listen | Select-Object OwningProcess
Stop-Process -Id <that id>
```
(Don't use `taskkill /im python.exe`; it would kill unrelated Python processes.) Verified 2026-10-21 (HTTP 200 and rendered values in a browser).

## F. Exact DAG demonstration sequence

1. Worker registered (`READ-ONLY`): `(Invoke-RestMethod http://localhost:8080/json/).aliveworkers` → `1`; cores in use should be 0.
2. JAR exists and matches (`READ-ONLY`): `docker exec spark-master md5sum /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar` → `1aba77af5cbbcb72b797c6ba7ad00116` (host build: `Get-FileHash -Algorithm MD5 src\scala\target\scala-2.12\bigdata-analytics-assembly-1.0.0.jar`).
3. Confirm the entry point supports the hold option (`READ-ONLY`): `Select-String -Path src\scala\benchmarking\CMUPerformanceBenchmarker.scala -Pattern "hold-ui"`.
4. Launch (`LONG-RUNNING`, ~1 minute of work + the hold; reads only). Open a PowerShell window and leave it running:
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.benchmarking.CMUPerformanceBenchmarker --executor-memory 1g --total-executor-cores 2 /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar --hold-ui 600
```
(The verified 2026-10-21 runs used `/tmp/bigdata-stage.jar`, a byte-identical copy of this JAR.) The line `Holding Spark Application UI open at http://localhost:4040` marks the point where the UI stays up for 600 s.
5. In another window confirm the application is live: `(Invoke-RestMethod http://localhost:8080/json/).activeapps` shows `CMUPerformanceBenchmarker-Phase19` RUNNING, and `http://localhost:4040/jobs/` returns 200.
6. Screenshots (project application only — check the title says **CMUPerformanceBenchmarker-Phase19**, not Spark Pi):
   - `http://localhost:4040/jobs/` — job list, all Succeeded.
   - `http://localhost:4040/stages/` — click a stage with several tasks (or a job link on the Jobs page) → **"DAG Visualization"** (expand it) for the stage graph.
   - `http://localhost:4040/SQL/` — click a query → the plan diagram.
   - `http://localhost:4040/executors/` — driver + executor 0 on the worker, 0 failed tasks.
   - `http://localhost:8080` — "Running Applications" shows the same application ID.
7. Verify counts and exit code after it ends (`READ-ONLY`), while the UI is still up:
```powershell
$id=(Invoke-RestMethod http://localhost:4040/api/v1/applications)[0].id
"jobs: " + (Invoke-RestMethod "http://localhost:4040/api/v1/applications/$id/jobs").Count
"stages: " + (Invoke-RestMethod "http://localhost:4040/api/v1/applications/$id/stages").Count
```
Process exit code: the `spark-submit` window ends and `echo $LASTEXITCODE` should print `0` (the verified run wrote `EXIT_CODE=0` to `evidence/dag_benchmark_run2.log`). After the hold, port 4040 closes and the Master lists the application as FINISHED with its executor shown as KILLED (normal teardown).

## G. Final health check  (`READ-ONLY`)

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
docker exec namenode hdfs dfsadmin -report | Select-String "Live datanodes|Under replicated|Missing blocks|corrupt"
docker exec namenode hdfs dfsadmin -safemode get
(Invoke-RestMethod http://localhost:8080/json/) | Select-Object status, aliveworkers
(Invoke-WebRequest -UseBasicParsing http://localhost:8501).StatusCode
docker exec namenode hdfs fsck /project/processed/hdfs_logs | Select-String "Status|Average block replication"
```

## H. Optional shutdown  (`START/STOP`)

Stop (not remove) only the Hadoop/Spark services; volumes and data are preserved and MongoDB is not named:
```powershell
docker compose -f docker\docker-compose.yml stop spark-worker spark-master datanode2 datanode1 namenode
```
Start again with the command in section A. **Never** use `docker compose down -v`, `docker volume rm`, or `docker system prune --volumes` — they delete the HDFS volumes (all data and models).

## I. Troubleshooting

| Symptom | Check | Fix |
|---|---|---|
| Master shows 0 workers | `docker logs spark-master --tail 20` ("unregistered worker") | `docker restart spark-worker` |
| 4040 refused | Is a driver running? `activeapps` empty | Expected; run section F |
| `Output directory ... already exists` | Job has fixed output path | Do not delete; choose a new output path (needs code or argument) |
| HDFS in safe mode right after start | `hdfs dfsadmin -safemode get` | Wait; it leaves automatically when blocks are reported |
| Under-replicated blocks right after a DataNode restart | `fsck /project` | Wait a few minutes; do not run `setrep` blindly |
| Dashboard 8501 down | `Get-NetTCPConnection -LocalPort 8501` | Restart with the E12 command |
| sbt fails on Java 25 | `java -version` in that shell | Use the JDK 17 block in section D |
| Executor OOM / job slow | Worker has 4 GB, 4 cores | Run one job at a time; lower `--executor-memory` |
| Docker commands not found in WSL | WSL integration off | Use Windows PowerShell |
