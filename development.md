# development.md — Engineering Diary
## Project: Distributed Hadoop Cluster Failure Prediction and Log Intelligence Platform
## Course: 23AID302 — Big Data Analytics
## Started: 2026-08-16
## Rule: Update after EVERY meaningful action. Record failures, rollbacks, and changes. Never falsify.

---

## 2026-08-16 — Phase 0 — Pre-flight

### Step 0.1 — Environment Inspection
**Status:** COMPLETE
**System:** Windows 11 Home Build 26200
**CPU:** Intel i7-13650HX — 14 cores / 20 logical
**RAM:** 16,069 MB
**Disk:** C: 69.37 GB free | D: 149.6 GB free | E: 294.33 GB free
**Docker:** 29.6.1 | **Docker Compose:** v5.2.0 | **Git:** 2.55.0 | **Python:** 3.10.0
**Java:** JDK 25.0.3 (local — MapReduce compile will happen inside namenode container)
**Scala/SBT:** Not installed locally — will compile inside spark-master container via Docker volume mount

### Step 0.2 — Project Structure Inspection
**Status:** COMPLETE
**CMU OpenCloud:** 20 monthly folders (2010-05 to 2011-12) | 120 files | 22.025 GB confirmed
**HDFS_v2:** 31 log files (1 NameNode 5.04 GB + 29 DataNodes + 1 SecondaryNameNode) | ~16.06 GB confirmed

### Step 0.3 — Dataset Verification (Schema + Row Counts)
**Status:** COMPLETE

**job.csv schema:** jtid, jobid, submitTime, launchTime, finishTime, status, numMaps, numReduces, finMaps, finReduces, failMaps, failReduces
**LEAKAGE BANNED COLUMNS:** finishTime, finMaps, finReduces, failMaps, failReduces (post-completion data)

**Status distribution (ALL 20 months):**
- SUCCESS (0): 49,026 — 88.74%
- FAILED  (1):  4,534 —  8.21%
- KILLED  (2):  1,666 —  3.01%
- OTHER       :     26 —  0.05%
- Total       : 55,252
- ML dataset after KILLED/OTHER exclusion: 53,560 records | Failure rate: 8.47%
- ACTION: RF and LR models must use class weighting. Primary metric = F1 + ROC-AUC.

**counter.csv:** Integer-encoded countergroup and counter keys — decode strategy TBD in Phase 4
**conf.csv:** Integer-encoded keyname — decode strategy TBD in Phase 4
**attempt.csv:** tasktype, attempt (retry count derivable — NOT leaked)
**split.csv:** Input split metadata — splitid count per job is safe feature

**HDFS_v2 log format:** YYYY-MM-DD HH:mm:ss,SSS LEVEL Component: Message
**WARNING:** Multi-line entries exist (STARTUP_MSG). Parser must detect new entries by timestamp prefix.
**NameNode logs:** 2016 era | DataNode logs: 2015 era

### Step 0.4/0.5 — Docker WSL Verification
**Status:** COMPLETE
**Result:** Docker virtual disks confirmed on E: drive
- E:\Docker\DockerDesktop\DockerDesktopWSL\disk\docker_data.vhdx — 1.48 GB
- E:\Docker\DockerDesktop\DockerDesktopWSL\main\ext4.vhdx — 0.09 GB
- C: drive holds ZERO Docker disk data — SAFE

### Step 0.6 — .wslconfig Created
**Status:** COMPLETE
**File:** C:\Users\S Ajay Vikram\.wslconfig
**Content:** memory=10GB, processors=8, swap=4GB, localhostForwarding=true

### Step 0.7 — Requirements Review
**Status:** COMPLETE — Project Constitution v2.0 approved before execution began

---
## NEXT: Step 0.8-0.11 — Initialize living documents (this file + 3 others)

---

## 2026-08-16 — Phase 0 (cont.) + Phase 1

### Steps 0.8–0.11 — Initialize Living Documents
**Status:** COMPLETE
**Files Created:**
- D:\BigData\Project_1\development.md (3 KB)
- D:\BigData\Project_1\requirements-traceability.md (1.4 KB)
- D:\BigData\Project_1\evidence-register.md (3.4 KB)
- D:\BigData\Project_1\commands_history.log (1.7 KB)
**Command:** Set-Content for each file → OK, verified with Get-ChildItem
**Errors:** None

### Step 0.12 — Conflict/Ambiguity Report
**Status:** COMPLETE (verbal)
**All conflicts resolved in Project Constitution v2.0:**
- Dashboard: Streamlit Phase 1, React optional Phase 2
- Anomaly detection: K-Means (Spark MLlib native)
- Compilation: Inside Docker containers, not locally
- docker-compose.yml: No version field (Compose v5 compatible)

---

## 2026-08-16 — Phase 1 — Git + Project Scaffold

### Phase 1 — Git Initialization
**Status:** COMPLETE
**Command:** git init → Initialized empty Git repository in D:/BigData/Project_1/.git/
**Branch:** master (default)
**Config:** user.name=S Ajay Vikram | user.email=ajay@project.local
**Errors:** None

### Phase 1 — Directory Structure
**Status:** COMPLETE
**Directories Created (all new):**
src/scala/{preprocessing,rdd,dataframe,dataset,ml,anomaly,streaming,drift}
src/mapreduce, src/dashboard, docker, docs, scripts, metrics
evidence/phase{0..19}, models, predictions, streaming_output, alerts, results
**Command:** New-Item -ItemType Directory -Force for each path → OK
**.gitkeep files:** Added to all empty directories so Git tracks structure
**Errors:** None

### Phase 1 — .gitignore
**Status:** COMPLETE
**Excludes:** cmu_opencloud/, hdfs_v2/, hdfs_v2_extracted/, models/, predictions/,
streaming_output/, results/, alerts/, *.parquet, *.class, *.jar, target/, .venv/, etc.
**Verified:** Raw data directories NOT staged — gitignore working correctly
**Errors:** None

### Phase 1 — README.md
**Status:** COMPLETE
**Content:** Project overview, tech stack, dataset table, key facts, docs index

### Phase 1 — First Git Commit
**Status:** COMPLETE
**Commit hash:** 28be075
**Message:** feat: initialize project scaffold and documentation structure
**Files committed:** 43 (docs + .gitkeep files + config)
**Raw data committed:** NO (gitignore confirmed working)
**CRLF Warning:** LF will be replaced by CRLF — Windows Git behavior, informational only
**Branch after commit:** develop (created and checked out)
**Branches:** master (stable), develop (current HEAD)
**Errors:** CRLF warnings — not errors, fix with .gitattributes in next commit

### Current Verified State (end of Phase 1)
- Git repo: initialized, 1 commit (28be075), on develop branch
- All directories: present on disk
- .gitignore: working — 22 GB raw data NOT in Git
- Living documents: all 4 present and initialized
- No Docker containers running yet
- No HDFS running yet
- Phase 2 approved: docker-compose.yml to be written next

## NEXT: Phase 2 — Write docker-compose.yml + hadoop.env + start cluster

### Phase 2.1 — Fix Spark Image Tags
**Status:** COMPLETE
**Command:** Edit docker-compose.yml (via multi_replace_file_content)
**Output:** bde2020/spark-master and spark-worker tags updated to 3.2.1-hadoop3.2
**Errors:** None (original 3.0.0 tag didn't exist)
**Git commit:** b4b1dc3 (fix: correct spark image tags in docker-compose.yml)

### Phase 2.2 — Docker Cluster Startup & Initialization
**Status:** COMPLETE
**Command:** docker-compose up -d
**Output:** namenode, datanode1, datanode2, spark-master, and spark-worker successfully pulled and started.
**Validation:** 
- docker exec namenode hdfs dfsadmin -report confirmed 2 Live DataNodes.
- HDFS directory structure created via hdfs dfs -mkdir -p /project/...
- Replication factor set to 2 via hdfs dfs -setrep -R 2 /project
**Evidence:** SS-01, SS-02, SS-03 (pending user capture)

### Phase 3 — Data Ingestion into HDFS
**Status:** COMPLETE
**Objective:** Upload 38 GB of raw data from local disk into the running HDFS cluster.
**Command:** 
- docker exec namenode bash -c "hdfs dfs -put /host-project/cmu_opencloud/20* /project/raw/cmu/"
- docker exec namenode bash -c "hdfs dfs -put /host-project/hdfs_v2_extracted/node_logs/* /project/raw/hdfs_v2/"
**Validation:**
- hdfs dfs -du -s -h /project/raw/cmu returned 22.0 G
- hdfs dfs -du -s -h /project/raw/hdfs_v2 returned 16.1 G
**Evidence:** SS-04, SS-05, SS-06 (pending user capture)

### Phase 4.1 — Data Validation (Schema Discovery)
**Status:** COMPLETE
**Objective:** Verify data schema inside HDFS.
**Command:** 
- hdfs dfs -cat /project/raw/cmu/2010-05/job.csv | head -n 5
- hdfs dfs -cat /project/raw/cmu/*/job.csv | wc -l
- hdfs dfs -cat /project/raw/hdfs_v2/hadoop-hdfs-datanode-mesos-01.log | head -n 5
**Validation:**
- job.csv schema exactly matches expected: jtid,jobid,submitTime,launchTime,finishTime,status,numMaps,numReduces,finMaps,finReduces,failMaps,failReduces
- Total job count: 55,252 records (55,272 lines with headers).
- Log file schema exactly matches expected: YYYY-MM-DD HH:mm:ss,SSS LEVEL Component: Message
- Multi-line logs identified (e.g., STARTUP_MSG).
**Evidence:** None required for this step.

### Phase 5.1 — Scala SBT Project Scaffold
**Status:** COMPLETE
**Objective:** Create SBT build configuration for Scala 2.12.15 + Spark 3.2.1.
**Files Created:**
- D:\BigData\Project_1\src\scala\build.sbt — Main build definition (Spark 3.2.1, Scala 2.12.15, sbt-assembly fat-jar config)
- D:\BigData\Project_1\src\scala\project\plugins.sbt — Adds sbt-assembly plugin v1.2.0
- D:\BigData\Project_1\src\scala\project\build.properties — Pins SBT launcher to v1.8.3
**Key Design Decisions:**
- Spark/Hadoop deps marked as provided — they are already in the cluster classpath
- Fat JAR excludes Spark/Hadoop/scala-library — prevents version conflicts at runtime
- MergeStrategy handles META-INF conflicts common in Spark fat-jar builds
- Compilation will happen INSIDE the spark-master container via volume mount (no local SBT needed)
**Errors:** None

### Phase 5.2 — CMUPreprocessor.scala
**Status:** WRITTEN (pending compilation + run)
**File:** D:\BigData\Project_1\src\scala\preprocessing\CMUPreprocessor.scala
**What it does:**
1. Reads all 20 months of job.csv from HDFS (/project/raw/cmu/*/job.csv) with explicit schema.
2. Drops 5 leakage columns: finishTime, finMaps, finReduces, failMaps, failReduces.
3. Filters: keeps only SUCCESS (0) and FAILED (1); excludes KILLED and OTHER.
4. Derives 3 safe features: queueWaitSecs, totalTasks, mapToReduceRatio.
5. Writes clean Parquet to /project/processed/cmu_jobs/ partitioned by label.
**Errors:** None (file created only — not yet compiled or run)

### Phase 5.3 — HDFSLogPreprocessor.scala
**Status:** WRITTEN (pending compilation + run)
**File:** D:\BigData\Project_1\src\scala\preprocessing\HDFSLogPreprocessor.scala
**What it does:**
1. Reads all 31 *.log files from HDFS (/project/raw/hdfs_v2/*.log).
2. Applies regex to parse timestamp, level, component, and message fields.
3. Filters out malformed/multi-line continuation lines.
4. Derives node_name from source filename.
5. Writes clean Parquet to /project/processed/hdfs_logs/ partitioned by level.
**Errors:** None (file created only — not yet compiled or run)
