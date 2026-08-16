package edu.project1.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * CMUSQLProcessor -- Phase 9 (Spark SQL Processing)
 *
 * Demonstrates Spark SQL capabilities for querying semi-structured data.
 * Registers temp views for job.csv, task.csv, attempt.csv.
 * Performs all aggregations and joins entirely via raw SQL strings.
 *
 * Output is written to: /project/batch_results/sql_output/
 *
 * Usage (inside spark-master container):
 *   /spark/bin/spark-submit --master spark://spark-master:7077
 *     --class edu.project1.sql.CMUSQLProcessor
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMUSQLProcessor {

  val RAW_CMU_PATH = "hdfs://namenode:9000/project/raw/cmu"
  val OUTPUT_PATH  = "hdfs://namenode:9000/project/batch_results/sql_output"

  // We explicitly define schemas here to match previous jobs, avoiding inference.
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

  val ATTEMPT_SCHEMA = StructType(Seq(
    StructField("jtid", LongType, true),
    StructField("taskid", LongType, false),
    StructField("attemptid", LongType, false),
    StructField("taskType", StringType, true),
    StructField("startTime", LongType, true),
    StructField("finishTime", LongType, true),
    StructField("status", IntegerType, true),
    StructField("hostname", StringType, true)
  ))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CMUSQLProcessor-Phase9")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val wallStart = System.currentTimeMillis()

    println("=" * 65)
    println("CMUSQLProcessor -- Phase 9 -- Spark SQL")
    println(s"Input  : $RAW_CMU_PATH")
    println("=" * 65)

    // =========================================================
    // STEP 1: LOAD DATA AND REGISTER TEMP VIEWS
    // =========================================================
    spark.read.option("header", "true").option("mode", "DROPMALFORMED").schema(JOB_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/job.csv")
      .createOrReplaceTempView("jobs")

    spark.read.option("header", "true").option("mode", "DROPMALFORMED").schema(TASK_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/task.csv")
      .createOrReplaceTempView("tasks")

    spark.read.option("header", "true").option("mode", "DROPMALFORMED").schema(ATTEMPT_SCHEMA)
      .csv(s"$RAW_CMU_PATH/*/attempt.csv")
      .createOrReplaceTempView("attempts")

    // =========================================================
    // STEP 2: RAW SQL TRANSFORMATIONS
    // =========================================================
    
    // We execute the aggregation and join logic completely in SQL
    val sqlQuery = """
      WITH CleanJobs AS (
        SELECT 
          jtid, 
          jobid,
          submitTime,
          launchTime,
          (launchTime - submitTime) AS queueWaitMs,
          status,
          numMaps,
          numReduces,
          (numMaps + numReduces) AS totalTasks
        FROM jobs
        WHERE status IN (0, 1)
      ),
      TaskAggs AS (
        SELECT 
          jtid, 
          jobid,
          COUNT(taskid) AS taskCount,
          SUM(CASE WHEN taskType = 'MAP' THEN 1 ELSE 0 END) AS mapTaskCount,
          SUM(CASE WHEN taskType = 'REDUCE' THEN 1 ELSE 0 END) AS reduceTaskCount,
          SUM(COALESCE(failures, 0)) AS taskFailureCount
        FROM tasks
        GROUP BY jtid, jobid
      ),
      AttemptAggs AS (
        -- Map attempt back to jobid through task table
        SELECT 
          t.jtid,
          t.jobid,
          COUNT(a.attemptid) AS totalAttempts,
          SUM(CASE WHEN a.status != 0 THEN 1 ELSE 0 END) AS failedAttemptCount
        FROM attempts a
        LEFT JOIN (SELECT DISTINCT jtid, taskid, jobid FROM tasks) t 
          ON a.jtid = t.jtid AND a.taskid = t.taskid
        WHERE t.jobid IS NOT NULL
        GROUP BY t.jtid, t.jobid
      )
      
      SELECT 
        j.jtid,
        j.jobid,
        j.queueWaitMs,
        j.totalTasks,
        ROUND(
          CAST(j.numMaps AS DOUBLE) / CAST(COALESCE(NULLIF(j.numReduces, 0), 1) AS DOUBLE), 
          4
        ) AS mapReduceRatio,
        COALESCE(t.taskCount, 0) AS taskCount,
        COALESCE(t.mapTaskCount, 0) AS mapTaskCount,
        COALESCE(t.reduceTaskCount, 0) AS reduceTaskCount,
        COALESCE(t.taskFailureCount, 0) AS taskFailureCount,
        COALESCE(a.totalAttempts, 0) AS totalAttempts,
        COALESCE(a.failedAttemptCount, 0) AS failedAttemptCount,
        CASE 
          WHEN COALESCE(a.totalAttempts, 0) = 0 THEN 0.0
          ELSE ROUND(CAST(a.failedAttemptCount AS DOUBLE) / CAST(a.totalAttempts AS DOUBLE), 4)
        END AS retryRate,
        j.status AS label
      FROM CleanJobs j
      LEFT JOIN TaskAggs t ON j.jtid = t.jtid AND j.jobid = t.jobid
      LEFT JOIN AttemptAggs a ON j.jtid = a.jtid AND j.jobid = a.jobid
      WHERE j.queueWaitMs >= 0
    """

    val resultDF = spark.sql(sqlQuery)
    resultDF.cache()
    
    val finalCount = resultDF.count()

    println("\n[INFO] Final SQL Schema (Execution Plan below):")
    resultDF.printSchema()
    
    println("\n[INFO] SQL Execution Plan (Explain):")
    resultDF.explain(extended = false)

    println(s"\n[INFO] Clean records: $finalCount")

    // =========================================================
    // STEP 3: WRITE OUTPUT
    // =========================================================
    println(s"\n[INFO] Writing Parquet to: $OUTPUT_PATH")
    resultDF
      .repartition(10)
      .write
      .mode("overwrite")
      .parquet(OUTPUT_PATH)

    val wallEnd    = System.currentTimeMillis()
    val elapsedMs  = wallEnd - wallStart
    val elapsedSec = elapsedMs / 1000.0

    println("\n" + "=" * 65)
    println("TIMING REPORT -- Spark SQL (Phase 9)")
    println("=" * 65)
    println(s"  Clean records        : $finalCount")
    println(s"  Elapsed (ms)         : $elapsedMs")
    println(s"  Elapsed (sec)        : ${"%.3f".format(elapsedSec)} s")
    println("=" * 65)
    println("Record this in: metrics/spark_sql_timing.txt")

    resultDF.unpersist()
    spark.stop()
  }
}
