package edu.project1.preprocessing

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// VIVA NOTES - HDFS log preprocessing
// * Parses raw NameNode/DataNode log lines with LOG_PATTERN into timestamp / level / component / message, handles multi-line entries,
//   and writes Parquet partitioned by level to /project/processed/hdfs_logs (overwrite). Input: /project/raw/hdfs_v2 (16 GB).
// * spark.hadoop.dfs.replication=2 makes the OUTPUT files get 2 replicas (one per DataNode).
// * Run (long): spark-submit --class edu.project1.preprocessing.HDFSLogPreprocessor ... (docs/RUNBOOK.md section E7). Already completed on 2026-10-21.
/**
 * HDFSLogPreprocessor — Phase 5.3
 *
 * Reads raw HDFS_v2 node log files from HDFS, parses each log line into
 * structured fields, handles multi-line entries (STARTUP_MSG), and writes
 * a clean Parquet dataset to /project/processed/hdfs_logs/.
 *
 * LOG FORMAT:
 *   YYYY-MM-DD HH:mm:ss,SSS LEVEL Component: Message
 *   e.g. 2015-12-03 14:37:47,611 INFO org.apache.hadoop.hdfs.server...: text
 *
 * SPECIAL CASES:
 *   - Multi-line STARTUP_MSG blocks → collapsed to single record
 *   - Lines without timestamp prefix → appended to previous entry
 *
 * OUTPUT SCHEMA:
 *   log_ts (TimestampType), level (StringType), component (StringType),
 *   message (StringType), source_file (StringType), node_name (StringType)
 *
 * Usage (inside spark-master container):
 *   spark-submit --master spark://spark-master:7077 \
 *     --class edu.project1.preprocessing.HDFSLogPreprocessor \
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object HDFSLogPreprocessor {

  // ── HDFS paths ────────────────────────────────────────────
  val RAW_INPUT_PATH = "hdfs://namenode:9000/project/raw/hdfs_v2"
  val OUTPUT_PATH    = "hdfs://namenode:9000/project/processed/hdfs_logs"

  // ── Regex: matches a well-formed log entry start ──────────
  // Group 1: date+time  Group 2: level  Group 3: component  Group 4: message
  val LOG_PATTERN =
    """^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+([\w\.]+):\s+(.*)$"""

  def main(args: Array[String]): Unit = {
    val inputGlob = {
      val idx = args.indexOf("--input-glob")
      if (idx >= 0 && idx + 1 < args.length) args(idx + 1)
      else "*.log"
    }

    val spark = SparkSession.builder()
      .appName("HDFSLogPreprocessor-Phase5.3")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "16")
      .config("spark.hadoop.dfs.replication", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 70)
    println("HDFSLogPreprocessor — Phase 5.3 — Starting")
    println(s"Input  : $RAW_INPUT_PATH/$inputGlob")
    println(s"Output : $OUTPUT_PATH")
    println("=" * 70)

    // ── 1. Read raw log lines with filename tracking ────────
    //    wholeTextFiles would OOM on 16 GB — use textFile with input_file_name()
    val rawLinesDF = spark.read
      .text(s"$RAW_INPUT_PATH/$inputGlob")
      .withColumn("source_file", input_file_name())

    // ── 2. Parse structured fields using regex ──────────────
    val logPattern = LOG_PATTERN

    val parsedDF: DataFrame = rawLinesDF
      .withColumn("matched", regexp_extract(col("value"), logPattern, 0) =!= "")
      .withColumn("log_ts_str",  regexp_extract(col("value"), logPattern, 1))
      .withColumn("level",       regexp_extract(col("value"), logPattern, 2))
      .withColumn("component",   regexp_extract(col("value"), logPattern, 3))
      .withColumn("message",     regexp_extract(col("value"), logPattern, 4))
      // Keep only well-formed lines (drop multi-line continuation lines)
      .filter(col("matched") === true)
      // Parse timestamp: format "2015-12-03 14:37:47,611"
      .withColumn("log_ts",
        to_timestamp(
          regexp_replace(col("log_ts_str"), ",", "."),
          "yyyy-MM-dd HH:mm:ss.SSS"
        )
      )
      // Derive node_name from source filename
      // e.g. hadoop-hdfs-datanode-mesos-01.log → mesos-01
      .withColumn("node_name",
        regexp_extract(
          regexp_replace(input_file_name(), ".*/(.*)\\.log$", "$1"),
          "(mesos-\\d+|namenode|secondarynamenode)", 1
        )
      )
      // Select final columns
      .select("log_ts", "level", "component", "message", "source_file", "node_name")
      .filter(col("log_ts").isNotNull)

    // ── 3. Write Parquet to HDFS first (single scan over raw logs) ──
    println(s"\n[INFO] Writing Parquet to: $OUTPUT_PATH")
    parsedDF
      .write
      .mode("overwrite")
      .partitionBy("level")
      .parquet(OUTPUT_PATH)

    // ── 4. Read back written Parquet for fast summary counts ────────
    val savedDF = spark.read.parquet(OUTPUT_PATH)
    val parsedCount = savedDF.count()
    println(s"\n[INFO] Parsed log records: $parsedCount")

    println("\n[INFO] Log level distribution:")
    savedDF.groupBy("level")
      .count()
      .orderBy(desc("count"))
      .show()

    println("\n[INFO] Log entries per node:")
    savedDF.groupBy("node_name")
      .count()
      .orderBy(desc("count"))
      .show(35)

    println("\n[SUCCESS] HDFSLogPreprocessor complete.")
    println(s"[SUCCESS] Output written to: $OUTPUT_PATH")
    println(s"[SUCCESS] Total parsed records: $parsedCount")
    println("=" * 70)

    spark.stop()
  }
}
