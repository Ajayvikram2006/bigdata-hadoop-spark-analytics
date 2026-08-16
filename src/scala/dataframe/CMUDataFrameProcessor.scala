package edu.project1.dataframe

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * CMUDataFrameProcessor -- Phase 7 (Spark DataFrame Processing)
 *
 * Reads CMU OpenCloud CSV files with EXPLICIT schemas (no inference cost).
 * Performs job-level joins with task.csv and attempt.csv.
 * Handles nulls, leakage columns, duplicates, and invalid statuses.
 * Caches the joined DataFrame (reused across multiple operations).
 * Writes clean Parquet dataset to HDFS /project/processed/cmu_jobs/.
 *
 * JOIN STRATEGY (no row explosion):
 *   task.csv    -> aggregated to job-level (taskCount, mapTaskCount, reduceTaskCount)
 *   attempt.csv -> aggregated to job-level (totalAttempts, retryCount)
 *   All aggregates LEFT JOINED onto job.csv -- jobs with no task data kept.
 *
 * LEAKAGE BANNED COLUMNS (post-completion -- never used for prediction):
 *   finishTime, finMaps, finReduces, failMaps, failReduces
 *
 * LABEL:
 *   status==0 (SUCCESS) -> label=0
 *   status==1 (FAILED)  -> label=1
 *   status==2 (KILLED) + OTHER -> excluded
 *
 * Usage (inside spark-master container):
 *   /spark/bin/spark-submit --master spark://spark-master:7077
 *     --class edu.project1.dataframe.CMUDataFrameProcessor
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMUDataFrameProcessor {

  // -- HDFS paths --
  val RAW_CMU_PATH  = "hdfs://namenode:9000/project/raw/cmu"
  val OUTPUT_PATH   = "hdfs://namenode:9000/project/processed/cmu_jobs"

  // -- Explicit schema for job.csv (avoids 22 GB inference cost) --
  val JOB_SCHEMA = StructType(Seq(
    StructField("jtid",        LongType,    nullable = false),
    StructField("jobid",       LongType,    nullable = false),
    StructField("submitTime",  LongType,    nullable = false),
    StructField("launchTime",  LongType,    nullable = false),
    StructField("finishTime",  LongType,    nullable = true),  // LEAKAGE - dropped
    StructField("status",      IntegerType, nullable = false),
    StructField("numMaps",     IntegerType, nullable = false),
    StructField("numReduces",  IntegerType, nullable = false),
    StructField("finMaps",     IntegerType, nullable = true),  // LEAKAGE - dropped
    StructField("finReduces",  IntegerType, nullable = true),  // LEAKAGE - dropped
    StructField("failMaps",    IntegerType, nullable = true),  // LEAKAGE - dropped
    StructField("failReduces", IntegerType, nullable = true)   // LEAKAGE - dropped
  ))

  // -- Explicit schema for task.csv --
  // Columns: jtid, jobid, taskid, type, startTime, finishTime, status, failures
  val TASK_SCHEMA = StructType(Seq(
    StructField("jtid",       LongType,    nullable = true),
    StructField("jobid",      LongType,    nullable = false),
    StructField("taskid",     LongType,    nullable = false),
    StructField("taskType",   StringType,  nullable = true),
    StructField("startTime",  LongType,    nullable = true),
    StructField("finishTime", LongType,    nullable = true),  // task level - not job leakage
    StructField("status",     IntegerType, nullable = true),
    StructField("failures",   IntegerType, nullable = true)
  ))

  // -- Explicit schema for attempt.csv --
  // Columns: jtid, taskid, attemptid, type, startTime, finishTime, status, hostname
  val ATTEMPT_SCHEMA = StructType(Seq(
    StructField("jtid",       LongType,    nullable = true),
    StructField("taskid",     LongType,    nullable = false),
    StructField("attemptid",  LongType,    nullable = false),
    StructField("taskType",   StringType,  nullable = true),
    StructField("startTime",  LongType,    nullable = true),
    StructField("finishTime", LongType,    nullable = true),
    StructField("status",     IntegerType, nullable = true),
    StructField("hostname",   StringType,  nullable = true)
  ))

  val LEAKAGE_COLS = Seq("finishTime", "finMaps", "finReduces", "failMaps", "failReduces")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CMUDataFrameProcessor-Phase7")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "20")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val wallStart = System.currentTimeMillis()

    println("=" * 65)
    println("CMUDataFrameProcessor -- Phase 7 -- Spark DataFrame")
    println(s"Input  : $RAW_CMU_PATH")
    println(s"Output : $OUTPUT_PATH")
    println("=" * 65)

    // =========================================================
    // STEP 1: READ job.csv with EXPLICIT schema
    // =========================================================
    val rawJobDF: DataFrame = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(JOB_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/job.csv")

    val rawJobCount = rawJobDF.count()
    println(s"\n[INFO] Raw job records: $rawJobCount")

    // =========================================================
    // STEP 2: DROP LEAKAGE COLUMNS + FILTER to SUCCESS/FAILED
    // =========================================================
    val jobDF: DataFrame = rawJobDF
      .drop(LEAKAGE_COLS: _*)
      .filter(col("status").isin(0, 1))  // binary: SUCCESS=0, FAILED=1

    val jobCount = jobDF.count()
    println(s"[INFO] After leakage drop + KILLED exclusion: $jobCount")

    // =========================================================
    // STEP 3: READ task.csv and AGGREGATE to job-level
    //         Never load all months into memory -- process per glob
    //         Aggregate: taskCount, mapCount, reduceCount, taskFailures
    // =========================================================
    val rawTaskDF: DataFrame = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(TASK_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/task.csv")

    // Aggregate task.csv to job level (no row explosion)
    val taskAggDF: DataFrame = rawTaskDF
      .groupBy("jtid", "jobid")
      .agg(
        count("taskid").as("taskCount"),
        sum(when(col("taskType") === "MAP", 1).otherwise(0)).as("mapTaskCount"),
        sum(when(col("taskType") === "REDUCE", 1).otherwise(0)).as("reduceTaskCount"),
        sum(coalesce(col("failures"), lit(0))).as("taskFailureCount")
      )

    println(s"[INFO] Task aggregates (job-level): ${taskAggDF.count()} jobs")

    // =========================================================
    // STEP 4: READ attempt.csv and AGGREGATE to job-level
    //         via taskid join (task -> jtid, jobid mapping)
    // =========================================================
    val rawAttemptDF: DataFrame = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(ATTEMPT_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/attempt.csv")

    // Join attempt with task to get jobid, then aggregate
    val taskIdMapDF = rawTaskDF.select("jtid", "taskid", "jobid").distinct()

    val attemptAggDF: DataFrame = rawAttemptDF
      .join(taskIdMapDF, Seq("jtid", "taskid"), "left")
      .filter(col("jobid").isNotNull)
      .groupBy("jtid", "jobid")
      .agg(
        count("attemptid").as("totalAttempts"),
        // retryCount: total attempts minus task count = retries
        // (safe proxy -- no future leakage)
        sum(when(col("status") =!= 0, 1).otherwise(0)).as("failedAttemptCount")
      )

    println(s"[INFO] Attempt aggregates (job-level): ${attemptAggDF.count()} jobs")

    // =========================================================
    // STEP 5: JOIN job + task agg + attempt agg (LEFT JOINS)
    //         Left join preserves all jobs even if task/attempt data missing
    // =========================================================
    val joinedDF: DataFrame = jobDF
      .join(taskAggDF,  Seq("jtid", "jobid"), "left")
      .join(attemptAggDF, Seq("jtid", "jobid"), "left")

    // Cache: this DataFrame is used in multiple downstream operations
    joinedDF.cache()
    val joinedCount = joinedDF.count()
    println(s"\n[INFO] Joined DataFrame records: $joinedCount")

    // =========================================================
    // STEP 6: FEATURE ENGINEERING (safe -- no leakage)
    //         All features known at job launch time
    // =========================================================
    val featuredDF: DataFrame = joinedDF
      // Queue wait time (ms): launchTime - submitTime
      .withColumn("queueWaitMs",
        col("launchTime") - col("submitTime"))
      // Total tasks (proxy for job complexity)
      .withColumn("totalTasks",
        coalesce(col("numMaps"), lit(0)) + coalesce(col("numReduces"), lit(0)))
      // Map-to-reduce ratio (task balance signal)
      .withColumn("mapReduceRatio",
        round(col("numMaps").cast(DoubleType) /
          (col("numReduces") + lit(1)).cast(DoubleType), 4))
      // Retry rate (failed attempts / total attempts)
      .withColumn("retryRate",
        when(col("totalAttempts").isNull || col("totalAttempts") === 0, lit(0.0))
          .otherwise(
            round(col("failedAttemptCount").cast(DoubleType) /
              col("totalAttempts").cast(DoubleType), 4)))
      // Binary label (status 0->0, status 1->1)
      .withColumn("label", col("status").cast(IntegerType))
      .drop("status")

    // =========================================================
    // STEP 7: NULL HANDLING + VALIDATION
    // =========================================================
    val cleanDF: DataFrame = featuredDF
      // Remove records with invalid queue wait (data quality)
      .filter(col("queueWaitMs") >= 0)
      // Fill nulls in joined columns (jobs with no task data)
      .na.fill(0, Seq("taskCount", "mapTaskCount", "reduceTaskCount",
                      "taskFailureCount", "totalAttempts", "failedAttemptCount",
                      "retryRate"))

    val cleanCount = cleanDF.count()
    println(s"[INFO] Clean records after null handling: $cleanCount")
    println(s"[INFO] Removed (invalid queueWait): ${joinedCount - cleanCount}")

    // =========================================================
    // STEP 8: PRINT SCHEMA (required for SS-15 evidence)
    // =========================================================
    println("\n[INFO] Final DataFrame schema:")
    cleanDF.printSchema()

    println("\n[INFO] Class distribution:")
    cleanDF.groupBy("label").count().orderBy("label").show()

    println("[INFO] Feature statistics:")
    cleanDF.select("queueWaitMs", "totalTasks", "mapReduceRatio",
                   "taskCount", "totalAttempts", "retryRate").describe().show()

    // =========================================================
    // STEP 9: WRITE Parquet to HDFS (partitioned by label)
    // =========================================================
    println(s"\n[INFO] Writing Parquet to: $OUTPUT_PATH")
    cleanDF
      .repartition(20, col("label"))
      .write
      .mode("overwrite")
      .partitionBy("label")
      .parquet(OUTPUT_PATH)

    val wallEnd    = System.currentTimeMillis()
    val elapsedMs  = wallEnd - wallStart
    val elapsedSec = elapsedMs / 1000.0

    println("\n" + "=" * 65)
    println("TIMING REPORT -- Spark DataFrame (Phase 7)")
    println("=" * 65)
    println(s"  Partitions (output)  : 20")
    println(s"  Clean records        : $cleanCount")
    println(s"  Elapsed (ms)         : $elapsedMs")
    println(s"  Elapsed (sec)        : ${"%.3f".format(elapsedSec)} s")
    println("=" * 65)
    println("Record this in: metrics/spark_df_timing.txt")

    joinedDF.unpersist()
    spark.stop()
  }
}
