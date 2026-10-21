# Organization Change Log — 2026-10-21 (final audit)

No code behaviour, data, models, HDFS content, containers or MongoDB were changed. Nothing was re-executed (no builds, tests or Spark jobs).

## Files moved (no content changes)
| From | To |
|---|---|
| `commands_history.log.bak_20261020_231100` | `archive/backups/./` |
| `development.md.bak_20261020_231100` | `archive/backups/./` |
| `docker/docker-compose.yml.bak_20261020_231100` | `archive/backups/docker/` |
| `docs/DOCKER_VIVA_RUNBOOK.md.bak_20261021_005000` | `archive/backups/docs/` |
| `docs/PROJECT_AUDIT.md.bak_20261021_005000` | `archive/backups/docs/` |
| `evidence-register.md.bak_20261021_005000` | `archive/backups/./` |
| `evidence-register.md.bak_20261021_final` | `archive/backups/./` |
| `FINAL_ACCEPTANCE_REPORT.md.bak_demo` | `archive/backups/./` |
| `FINAL_ACCEPTANCE_REPORT.md.bak_hdfslogs` | `archive/backups/./` |
| `metrics/ml_kmeans_anomaly.txt.bak_20261021_final` | `archive/backups/metrics/` |
| `PROJECT_RESUMPTION_REPORT.md.bak_20261020_233600` | `archive/backups/./` |
| `requirements-traceability.md.bak_20261021_005000` | `archive/backups/./` |
| `RUNBOOK_COMPLETE.md.bak_20261020_233600` | `archive/backups/./` |
| `screenshots_final/SCREENSHOT_INDEX.md.bak_20261020_233600` | `archive/backups/screenshots_final/` |
| `scripts/run_pipeline.ps1.bak_20261020_231100` | `archive/backups/scripts/` |
| `scripts/run_pipeline.ps1.bak_20261021_072600` | `archive/backups/scripts/` |
| `scripts/run_pipeline.sh.bak_20261020_231100` | `archive/backups/scripts/` |
| `scripts/run_pipeline.sh.bak_20261021_072600` | `archive/backups/scripts/` |
| `src/python/dashboard.py.bak_20261021_final` | `archive/backups/src/python/` |
| `src/scala/preprocessing/HDFSLogPreprocessor.scala.bak_20261021_072730` | `archive/backups/src/scala/preprocessing/` |
| `src/scala/streaming/CMUHistoricalStreaming.scala.bak_20261021_010400` | `archive/backups/src/scala/streaming/` |
| `RUNBOOK_COMPLETE.md` | `docs/archive/` |
| `PROJECT_RESUMPTION_REPORT.md` | `docs/archive/` |
| `docs/DOCKER_VIVA_RUNBOOK.md` | `docs/archive/` |
| `demo.scala` | `archive/misc/` |

## Files edited (backup of the previous version kept under `archive/backups/`, suffix noted)
| File | Change | Backup |
|---|---|---|
| `docs/archive/RUNBOOK_COMPLETE.md`, `docs/archive/DOCKER_VIVA_RUNBOOK.md` | "SUPERSEDED" banner prepended | `archive/backups/docs/archive/*.bak_pre_banner` |
| `screenshots_final/SCREENSHOT_INDEX.md` | one path updated to `docs/archive/RUNBOOK_COMPLETE.md` | `archive/backups/screenshots_final/SCREENSHOT_INDEX.md.bak_pre_organize` |
| 17 source files (Scala ×13, Java ×3, Python ×2 — see `docs/PROJECT_MAP.md`) | **Comment-only** "VIVA NOTES" blocks inserted before the class/object (or top of the Python file); verified by diff that no non-comment line changed | `archive/backups/<same path>.bak_pre_comments` |
| `README.md` | "Start here" pointer to runbook/map | `archive/backups/README.md.bak_pre_organize` |
| `FINAL_ACCEPTANCE_REPORT.md`, `evidence-register.md` | final-audit sections appended | `archive/backups/*.bak_pre_audit` |

## Files created
`docs/RUNBOOK.md`, `docs/PROJECT_MAP.md`, `docs/ORGANIZATION_CHANGE_LOG.md`.

## Deliberately not moved (references or runtime paths would break)
Source tree and `src/scala/target` (sbt/Docker paths), `docker/`, `scripts/`, `metrics/` (dashboard reads it by relative path), raw datasets and `evidence/`/`screenshots_final/` (referenced by reports), `development.md`, `requirements-traceability.md`, `README.md`, `evidence-register.md` and `FINAL_ACCEPTANCE_REPORT.md` (referenced from other documents). `commands_history.log` and `development.md` still mention the old locations of moved documents; they are historical logs and were left unedited.

## Not re-executed
The JAR (`bigdata-analytics-assembly-1.0.0.jar`) was built before the comment-only edits, so it is slightly older than the sources; comments do not change behaviour but the JAR was not rebuilt and no test was re-run.

---
# Restructure into a pipeline-flow layout (second pass)

Nothing was built, tested or run. Behaviour of the code is unchanged; `build.sbt` was edited so sbt finds the new stage folders (**this edit has not been compiled yet** - run the sbt command in `docs/RUNBOOK.md` section D once to confirm; backup `archive/backups/src/scala/build.sbt.bak_pre_restructure`).

## Old -> new locations
| Old | New |
|---|---|
| `cmu_opencloud` | `data/raw/cmu_opencloud` |
| `hdfs_v2` | `data/raw/hdfs_v2` |
| `hdfs_v2_extracted` | `data/raw/hdfs_v2_extracted` |
| `models` | `data/local_outputs/models` |
| `predictions` | `data/local_outputs/predictions` |
| `results` | `data/local_outputs/results` |
| `alerts` | `data/local_outputs/alerts` |
| `streaming_output` | `data/local_outputs/streaming_output` |
| `FINAL_ACCEPTANCE_REPORT.md` | `docs/reports/FINAL_ACCEPTANCE_REPORT.md` |
| `evidence-register.md` | `docs/reports/evidence-register.md` |
| `requirements-traceability.md` | `docs/reports/requirements-traceability.md` |
| `docs/PROJECT_AUDIT.md` | `docs/reports/PROJECT_AUDIT.md` |
| `development.md` | `docs/history/development.md` |
| `commands_history.log` | `docs/history/commands_history.log` |
| `screenshots_final` | `evidence/final_screenshots` |
| `src/scala/preprocessing/CMUPreprocessor.scala` | `src/scala/01_preprocessing/CMUPreprocessor.scala` |
| `src/scala/preprocessing/HDFSLogPreprocessor.scala` | `src/scala/01_preprocessing/HDFSLogPreprocessor.scala` |
| `src/scala/rdd/RDDJobParser.scala` | `src/scala/02_batch_processing/rdd/RDDJobParser.scala` |
| `src/scala/dataframe/CMUDataFrameProcessor.scala` | `src/scala/02_batch_processing/dataframe/CMUDataFrameProcessor.scala` |
| `src/scala/dataset/CMUDatasetProcessor.scala` | `src/scala/02_batch_processing/dataset/CMUDatasetProcessor.scala` |
| `src/scala/sql/CMUSQLProcessor.scala` | `src/scala/02_batch_processing/sql/CMUSQLProcessor.scala` |
| `src/scala/ml/CMULogisticRegression.scala` | `src/scala/03_machine_learning/CMULogisticRegression.scala` |
| `src/scala/ml/CMURandomForest.scala` | `src/scala/03_machine_learning/CMURandomForest.scala` |
| `src/scala/ml/CMUKMeansAnomaly.scala` | `src/scala/04_anomaly_and_drift/CMUKMeansAnomaly.scala` |
| `src/scala/ml/CMUDriftDetection.scala` | `src/scala/04_anomaly_and_drift/CMUDriftDetection.scala` |
| `src/scala/streaming/CMUHistoricalStreaming.scala` | `src/scala/05_streaming/CMUHistoricalStreaming.scala` |
| `src/scala/streaming/CMUStreamingJob.scala` | `src/scala/05_streaming/CMUStreamingJob.scala` |
| `src/scala/benchmarking/CMUPerformanceBenchmarker.scala` | `src/scala/06_benchmark_dag/CMUPerformanceBenchmarker.scala` |
| `src/scala/preprocessing` | `archive/misc/legacy_scala_dirs/preprocessing` |
| `src/scala/rdd` | `archive/misc/legacy_scala_dirs/rdd` |
| `src/scala/dataframe` | `archive/misc/legacy_scala_dirs/dataframe` |
| `src/scala/dataset` | `archive/misc/legacy_scala_dirs/dataset` |
| `src/scala/sql` | `archive/misc/legacy_scala_dirs/sql` |
| `src/scala/ml` | `archive/misc/legacy_scala_dirs/ml` |
| `src/scala/streaming` | `archive/misc/legacy_scala_dirs/streaming` |
| `src/scala/benchmarking` | `archive/misc/legacy_scala_dirs/benchmarking` |
| `src/scala/anomaly` | `archive/misc/legacy_scala_dirs/anomaly` |
| `src/scala/drift` | `archive/misc/legacy_scala_dirs/drift` |
| `src/dashboard` | `archive/misc/legacy_scala_dirs/src_dashboard_placeholder` |

## Text references updated (backups in `archive/backups/pre_restructure/`)
`README.md`, `docs/RUNBOOK.md`, `docs/PROJECT_MAP.md` (rewritten), `docs/reports/*`, `evidence/final_screenshots/SCREENSHOT_INDEX.md`, `.gitignore`. `docs/history/*` and `docs/archive/*` are historical and still use old paths.

## Added
Folder `README.md` files in `data/`, `data/raw/`, `data/local_outputs/`, each `src/scala/0x_*` stage, `src/mapreduce`, `src/python`, `scripts`, `docker`, `metrics`, `evidence`, `docs/reports`, `docs/history`, `archive`.

## Not moved on purpose
`src/mapreduce`, `src/python` (referenced by `scripts/*.sh` and the dashboard `../../metrics` lookup), `metrics/`, `docker/`, `scripts/` (container paths `/host-project/...` and scripts reference them), `src/scala/target`, `project/`. The Docker mount (`D:/BigData/Project_1:/host-project`) is unaffected; no script referenced the moved folders.
