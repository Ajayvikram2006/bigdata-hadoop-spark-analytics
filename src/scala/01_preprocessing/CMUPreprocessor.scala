package edu.project1.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// VIVA NOTES - CMU preprocessing
// * Cleans the raw CMU job CSVs and writes Parquet (overwrite) to /project/processed/cmu_jobs - the same output path as CMUDataFrameProcessor.
// * Run: spark-submit --class edu.project1.preprocessing.CMUPreprocessor (docs/RUNBOOK.md section E3).
/**
 * CMUPreprocessor — Phase 5.2
 *
 * Reads raw CMU OpenCloud job.csv files from HDFS, drops post-completion
 * leakage columns, derives safe features and a binary label, and writes
 * a clean Parquet dataset to /project/processed/cmu_jobs/.
 *
 * LEAKAGE BANNED COLUMNS (post-completion data — never use for prediction):
 *   finishTime, finMaps, finReduces, failMaps, failReduces
 *
 * LABEL DEFINITION:
 *   status == 0 (SUCCESS) → label = 0
 *   status == 1 (FAILED)  → label = 1
 *   status == 2 (KILLED) and OTHER → EXCLUDED (not ambiguous enough for binary ML)
 *
 * SAFE INPUT FEATURES (known at job launch time only):
 *   jtid, jobid, submitTime, launchTime, numMaps, numReduces
 *   + derived: queueWaitSecs, mapToReduceRatio, totalTasks
 *
 * Usage (inside spark-master container):
 *   spark-submit --master spark://spark-master:7077 \
 *     --class edu.project1.preprocessing.CMUPreprocessor \
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMUPreprocessor {

  // ── HDFS paths ────────────────────────────────────────────
  val RAW_INPUT_PATH  = "hdfs://namenode:9000/project/raw/cmu"
  val OUTPUT_PATH     = "hdfs://namenode:9000/project/processed/cmu_jobs"

  // ── Explicit schema (avoids inference cost on 22 GB) ───────
  val JOB_SCHEMA = StructType(Seq(
    StructField("jtid",        LongType,   nullable = false),
    StructField("jobid",       LongType,   nullable = false),
    StructField("submitTime",  LongType,   nullable = false),
    StructField("launchTime",  LongType,   nullable = false),
    StructField("finishTime",  LongType,   nullable = true),  // LEAKAGE — will be dropped
    StructField("status",      IntegerType, nullable = false),
    StructField("numMaps",     IntegerType, nullable = false),
    StructField("numReduces",  IntegerType, nullable = false),
    StructField("finMaps",     IntegerType, nullable = true),  // LEAKAGE — will be dropped
    StructField("finReduces",  IntegerType, nullable = true),  // LEAKAGE — will be dropped
    StructField("failMaps",    IntegerType, nullable = true),  // LEAKAGE — will be dropped
    StructField("failReduces", IntegerType, nullable = true)   // LEAKAGE — will be dropped
  ))

  // ── Leakage columns — banned from all downstream ML ───────
  val LEAKAGE_COLS = Seq("finishTime", "finMaps", "finReduces", "failMaps", "failReduces")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CMUPreprocessor-Phase5.2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "50")
      .config("spark.hadoop.dfs.replication", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("=" * 70)
    println("CMUPreprocessor — Phase 5.2 — Starting")
    println(s"Input  : $RAW_INPUT_PATH")
    println(s"Output : $OUTPUT_PATH")
    println("=" * 70)

    // ── 1. Read all 20 months of CSV files ─────────────────
    val rawDF: DataFrame = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(JOB_SCHEMA)
      .csv(s"$RAW_INPUT_PATH/*/job.csv")

    val rawCount = rawDF.count()
    println(s"\n[INFO] Raw records loaded: $rawCount")

    // ── 2. Drop leakage columns ─────────────────────────────
    val noLeakDF = rawDF.drop(LEAKAGE_COLS: _*)
    println(s"[INFO] Leakage columns dropped: ${LEAKAGE_COLS.mkString(", ")}")

    // ── 3. Filter: keep only SUCCESS (0) and FAILED (1) ────
    val filteredDF = noLeakDF.filter(col("status").isin(0, 1))
    val filteredCount = filteredDF.count()
    println(s"[INFO] Records after KILLED/OTHER exclusion: $filteredCount")
    println(s"[INFO] Excluded records: ${rawCount - filteredCount}")

    // ── 4. Derive safe features ──────────────────────────────
    //
    //   queueWaitSecs  = (launchTime - submitTime) / 1000.0
    //                    Time job waited in queue before launch.
    //                    Units: seconds. Known at launch time.
    //
    //   totalTasks     = numMaps + numReduces
    //                    Total task count — proxy for job complexity.
    //
    //   mapToReduceRatio = numMaps / (numReduces + 1)
    //                    Task ratio — avoid div-by-zero with +1.
    //                    Reduce-heavy vs map-heavy job signal.
    //
    //   label          = status (0=SUCCESS, 1=FAILED) — the ML target.
    //
    val featuredDF = filteredDF
      .withColumn("queueWaitSecs",
        round((col("launchTime") - col("submitTime")) / 1000.0, 3))
      .withColumn("totalTasks",
        col("numMaps") + col("numReduces"))
      .withColumn("mapToReduceRatio",
        round(col("numMaps") / (col("numReduces") + 1).cast(DoubleType), 4))
      .withColumn("label",
        col("status").cast(IntegerType))
      // Clean up: drop raw status col (label replaces it)
      .drop("status")

    // ── 5. Filter out invalid rows (negative wait times) ───
    val cleanDF = featuredDF.filter(col("queueWaitSecs") >= 0)
    val cleanCount = cleanDF.count()
    println(s"[INFO] Records after invalid-row filter: $cleanCount")

    // ── 6. Class imbalance report ───────────────────────────
    println("\n[INFO] Class distribution in clean dataset:")
    cleanDF.groupBy("label")
      .count()
      .orderBy("label")
      .show()

    // ── 7. Write Parquet to HDFS ────────────────────────────
    println(s"\n[INFO] Writing Parquet to: $OUTPUT_PATH")
    cleanDF
      .coalesce(20)   // 20 partitions — ~1 per month, manageable file sizes
      .write
      .mode("overwrite")
      .partitionBy("label")
      .parquet(OUTPUT_PATH)

    println("\n[SUCCESS] CMUPreprocessor complete.")
    println(s"[SUCCESS] Output written to: $OUTPUT_PATH")
    println(s"[SUCCESS] Total clean records: $cleanCount")
    println("=" * 70)

    spark.stop()
  }
}
