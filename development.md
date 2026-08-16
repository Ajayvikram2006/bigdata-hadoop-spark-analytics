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
