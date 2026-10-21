package edu.project1.dataset

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * CMUDatasetProcessor -- Phase 8 (Spark Dataset Processing)
 *
 * Demonstrates the strongly-typed Dataset API.
 * Defines case classes for type-safe manipulation of OpenCloud data.
 * Computes the exact same features as the DataFrame implementation
 * to allow for a direct 1-to-1 performance comparison (Phase 19).
 *
 * Output is written to: /project/batch_results/dataset_output/
 *
 * Usage (inside spark-master container):
 *   /spark/bin/spark-submit --master spark://spark-master:7077
 *     --class edu.project1.dataset.CMUDatasetProcessor
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */

// -- Typed Case Classes for Schema Enforcement --
case class JobRecord(
  jtid: Long,
  jobid: Long,
  submitTime: Long,
  launchTime: Long,
  status: Int,
  numMaps: Int,
  numReduces: Int
)

case class TaskAggRecord(
  jtid: Long,
  jobid: Long,
  taskCount: Long,
  mapTaskCount: Long,
  reduceTaskCount: Long,
  taskFailureCount: Long
)

case class JoinedJobRecord(
  jtid: Long,
  jobid: Long,
  queueWaitMs: Long,
  totalTasks: Int,
  mapReduceRatio: Double,
  taskFailureCount: Long,
  label: Int
)

// VIVA NOTES - Dataset API
// * Dataset[T] = DataFrame with compile-time types (case classes JobRecord / TaskAggRecord above); encoders convert rows <-> objects.
//   Gives type safety and lambda-style operations at the cost of some serialization overhead (shown in the benchmark).
// * Output: Parquet (overwrite) at /project/batch_results/dataset_output.
// * Run: spark-submit --class edu.project1.dataset.CMUDatasetProcessor (docs/RUNBOOK.md section E4).
object CMUDatasetProcessor {

  val RAW_CMU_PATH = "hdfs://namenode:9000/project/raw/cmu"
  val OUTPUT_PATH  = "hdfs://namenode:9000/project/batch_results/dataset_output"

  // Raw schemas needed to safely load data before applying `.as[CaseClass]`
  val JOB_SCHEMA = StructType(Seq(
    StructField("jtid", LongType, false),
    StructField("jobid", LongType, false),
    StructField("submitTime", LongType, false),
    StructField("launchTime", LongType, false),
    StructField("finishTime", LongType, true), // leakage
    StructField("status", IntegerType, false),
    StructField("numMaps", IntegerType, false),
    StructField("numReduces", IntegerType, false),
    StructField("finMaps", IntegerType, true), // leakage
    StructField("finReduces", IntegerType, true), // leakage
    StructField("failMaps", IntegerType, true), // leakage
    StructField("failReduces", IntegerType, true) // leakage
  ))

  val TASK_SCHEMA = StructType(Seq(
    StructField("jtid", LongType, true),
    StructField("jobid", LongType, false),
    StructField("taskid", LongType, false),
    StructField("taskType", StringType, true),
    StructField("startTime", LongType, true),
    StructField("finishTime", LongType, true),
    StructField("status", IntegerType, true),
    StructField("failures", IntegerType, true)
  ))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CMUDatasetProcessor-Phase8")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "20")
      .config("spark.hadoop.dfs.replication", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val wallStart = System.currentTimeMillis()

    println("=" * 65)
    println("CMUDatasetProcessor -- Phase 8 -- Spark Dataset API")
    println(s"Input  : $RAW_CMU_PATH")
    println("=" * 65)

    // =========================================================
    // STEP 1: LOAD JOB DATA AS DATASET[JobRecord]
    // =========================================================
    val jobDS: Dataset[JobRecord] = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(JOB_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/job.csv")
      // Drop leakage before casting to domain object
      .drop("finishTime", "finMaps", "finReduces", "failMaps", "failReduces")
      // Filter for known labels (SUCCESS=0, FAILED=1)
      .filter(col("status").isin(0, 1))
      .as[JobRecord]

    println(s"[INFO] Job Dataset records: ${jobDS.count()}")

    // =========================================================
    // STEP 2: LOAD TASK DATA AND AGGREGATE
    // =========================================================
    // Note: Grouping in Dataset API yields a KeyValueGroupedDataset,
    // which requires returning to DataFrame logic for complex aggregations,
    // then re-casting back to a Dataset.
    val taskAggDS: Dataset[TaskAggRecord] = spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(TASK_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/task.csv")
      .groupBy("jtid", "jobid")
      .agg(
        count("taskid").as("taskCount"),
        sum(when(col("taskType") === "MAP", 1).otherwise(0)).as("mapTaskCount"),
        sum(when(col("taskType") === "REDUCE", 1).otherwise(0)).as("reduceTaskCount"),
        sum(coalesce(col("failures"), lit(0))).as("taskFailureCount")
      )
      .as[TaskAggRecord]

    println(s"[INFO] Task Agg Dataset records: ${taskAggDS.count()}")

    // =========================================================
    // STEP 3: TYPE-SAFE JOIN (Dataset API)
    // =========================================================
    // Using `joinWith` creates a Dataset[(JobRecord, TaskAggRecord)]
    val joinedDS = jobDS.joinWith(
      taskAggDS,
      jobDS("jtid") === taskAggDS("jtid") && jobDS("jobid") === taskAggDS("jobid"),
      "left"
    )

    // =========================================================
    // STEP 4: MAP OVER TUPLES TO ENGINEER FEATURES
    // =========================================================
    val finalDS: Dataset[JoinedJobRecord] = joinedDS.map {
      case (job, taskAgg) =>
        
        val qWaitMs = job.launchTime - job.submitTime
        val tTasks = job.numMaps + job.numReduces
        val mrRatio = if (job.numReduces == 0) {
          math.round((job.numMaps.toDouble) * 10000.0) / 10000.0
        } else {
          math.round((job.numMaps.toDouble / job.numReduces.toDouble) * 10000.0) / 10000.0
        }

        // Handle nulls on the right side of the left join
        val failCount = if (taskAgg == null) 0L else taskAgg.taskFailureCount

        JoinedJobRecord(
          jtid = job.jtid,
          jobid = job.jobid,
          queueWaitMs = qWaitMs,
          totalTasks = tTasks,
          mapReduceRatio = mrRatio,
          taskFailureCount = failCount,
          label = job.status // 0=SUCCESS, 1=FAILED
        )
    }.filter(r => r.queueWaitMs >= 0) // data quality filter

    finalDS.cache()
    val finalCount = finalDS.count()

    println("\n[INFO] Final Dataset schema:")
    finalDS.printSchema()

    println(s"\n[INFO] Clean records: $finalCount")

    // =========================================================
    // STEP 5: WRITE OUTPUT
    // =========================================================
    println(s"\n[INFO] Writing Parquet to: $OUTPUT_PATH")
    finalDS
      .repartition(10) // smaller output for demo
      .write
      .mode("overwrite")
      .parquet(OUTPUT_PATH)

    val wallEnd    = System.currentTimeMillis()
    val elapsedMs  = wallEnd - wallStart
    val elapsedSec = elapsedMs / 1000.0

    println("\n" + "=" * 65)
    println("TIMING REPORT -- Spark Dataset (Phase 8)")
    println("=" * 65)
    println(s"  Clean records        : $finalCount")
    println(s"  Elapsed (ms)         : $elapsedMs")
    println(s"  Elapsed (sec)        : ${"%.3f".format(elapsedSec)} s")
    println("=" * 65)
    println("Record this in: metrics/spark_ds_timing.txt")

    finalDS.unpersist()
    spark.stop()
  }
}
