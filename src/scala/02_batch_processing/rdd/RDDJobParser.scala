package edu.project1.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// VIVA NOTES - RDD (Resilient Distributed Dataset)
// * RDD = immutable, partitioned collection processed in parallel. Transformations (filter, map, mapValues, reduceByKey) are LAZY;
//   only an ACTION (collect, count, saveAsTextFile) starts a Spark job. reduceByKey causes a shuffle = a stage boundary in the DAG.
// * Flow: read CSV lines from /project/raw/cmu -> drop header/malformed lines (filter) -> (status, 1) pairs -> reduceByKey(_ + _) -> counts.
// * Output goes to FIXED paths OUTPUT_STATUS_PATH / OUTPUT_QWAIT_PATH. saveAsTextFile FAILS if the directory exists, so a rerun needs a new
//   output path (never delete the existing results).
// * Run: docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class edu.project1.rdd.RDDJobParser /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
//   See docs/RUNBOOK.md section E2.
/**
 * RDDJobParser -- Phase 6 (Spark RDD Processing)
 *
 * Reads CMU job.csv files from HDFS using raw Spark RDDs.
 * Demonstrates the full suite of RDD transformations:
 *   - textFile          (load)
 *   - filter            (skip headers and malformed rows)
 *   - map               (parse CSV line -> typed tuple)
 *   - flatMap           (extract multiple features per record)
 *   - reduceByKey       (aggregate counts by status)
 *   - mapValues         (compute percentage from count)
 *   - sortBy            (order results)
 *
 * OUTPUT written to HDFS:
 *   /project/batch_results/rdd_status_counts/   -- status distribution
 *   /project/batch_results/rdd_queue_stats/     -- queueWait stats per status
 *
 * Timing is printed to stdout and saved to metrics/spark_rdd_timing.txt via redirect.
 *
 * Usage (inside spark-master container):
 *   spark-submit --master spark://spark-master:7077
 *     --class edu.project1.rdd.RDDJobParser
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object RDDJobParser {

  // -- HDFS paths --
  val RAW_INPUT_PATH        = "hdfs://namenode:9000/project/raw/cmu"
  val OUTPUT_STATUS_PATH    = "hdfs://namenode:9000/project/batch_results/rdd_status_counts"
  val OUTPUT_QWAIT_PATH     = "hdfs://namenode:9000/project/batch_results/rdd_queue_stats"

  // -- Column indices (0-based) --
  val COL_SUBMIT_TIME = 2
  val COL_LAUNCH_TIME = 3
  val COL_STATUS      = 5
  val COL_NUM_MAPS    = 6
  val COL_NUM_REDUCES = 7

  // -- Status codes --
  val STATUS_SUCCESS = 0
  val STATUS_FAILED  = 1
  val STATUS_KILLED  = 2

  // -- Status label map (broadcast-friendly plain object) --
  val STATUS_LABELS = Map(
    STATUS_SUCCESS -> "SUCCESS",
    STATUS_FAILED  -> "FAILED",
    STATUS_KILLED  -> "KILLED"
  )

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("RDDJobParser-Phase6")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.hadoop.dfs.replication", "2")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    println("=" * 65)
    println("RDDJobParser -- Phase 6 -- Spark RDD Processing")
    println(s"Input  : $RAW_INPUT_PATH")
    println("=" * 65)

    val wallStart = System.currentTimeMillis()

    // =========================================================
    // STEP 1: LOAD raw text lines from all 20 months
    // RDD[String] -- one element per line across all job.csv files
    // =========================================================
    val rawRDD: RDD[String] = sc.textFile(s"$RAW_INPUT_PATH/*/job.csv")

    val rawPartitions = rawRDD.getNumPartitions
    println(s"\n[INFO] Raw RDD partitions: $rawPartitions")
    println(s"[INFO] Raw RDD total lines: ${rawRDD.count()}")

    // =========================================================
    // STEP 2: FILTER -- remove headers and SASL log lines
    // Demonstrates: filter transformation
    // =========================================================
    val dataRDD: RDD[String] = rawRDD
      .filter(line => {
        val trimmed = line.trim
        trimmed.nonEmpty &&
        !trimmed.startsWith("jtid") &&       // CSV header
        !trimmed.startsWith("20") || trimmed.contains(",")  // keep data rows
      })
      .filter(line => line.contains(","))    // must be CSV (has commas)

    // =========================================================
    // STEP 3: MAP -- parse CSV line -> (statusCode, queueWaitMs)
    // Demonstrates: map transformation
    // Returns None for malformed rows (handled below with flatMap)
    // =========================================================
    val parsedRDD: RDD[(Int, Long)] = dataRDD.flatMap(line => {
      val fields = line.split(",", -1)
      if (fields.length <= COL_NUM_REDUCES) {
        None  // flatMap on Option -- malformed rows silently dropped
      } else {
        try {
          val statusCode  = fields(COL_STATUS).trim.toInt
          val submitTime  = fields(COL_SUBMIT_TIME).trim.toLong
          val launchTime  = fields(COL_LAUNCH_TIME).trim.toLong
          val queueWaitMs = launchTime - submitTime
          // Only keep known binary-label statuses for ML-relevant stats
          Some((statusCode, queueWaitMs))
        } catch {
          case _: NumberFormatException => None
        }
      }
    })

    // Cache -- parsedRDD is used in TWO separate action chains below
    parsedRDD.cache()
    val totalParsed = parsedRDD.count()
    println(s"[INFO] Parsed records (after flatMap): $totalParsed")

    // =========================================================
    // STEP 4: REDUCE BY KEY -- count records per status
    // Demonstrates: mapValues + reduceByKey transformations
    // =========================================================
    val statusCountRDD: RDD[(String, Long)] = parsedRDD
      .map { case (statusCode, _) =>
        // Map status code to label using getOrElse for safety
        val label = STATUS_LABELS.getOrElse(statusCode, s"UNKNOWN_$statusCode")
        (label, 1L)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)  // highest count first

    println("\n[INFO] Status distribution (RDD reduceByKey):")
    val statusResults = statusCountRDD.collect()
    statusResults.foreach { case (label, count) =>
      val pct = count * 100.0 / totalParsed
      println(f"  $label%-12s : $count%6d  ($pct%.2f%%)")
    }

    // =========================================================
    // STEP 5: AGGREGATE queueWait stats per status
    // Demonstrates: flatMap -> reduceByKey with compound accumulator
    // Accumulator: (sumMs, count, minMs, maxMs)
    // =========================================================
    val queueStatsRDD: RDD[(String, (Long, Long, Long, Long))] = parsedRDD
      .filter { case (statusCode, queueWaitMs) =>
        queueWaitMs >= 0 && STATUS_LABELS.contains(statusCode)
      }
      .map { case (statusCode, queueWaitMs) =>
        val label = STATUS_LABELS(statusCode)
        // (sum, count, min, max)
        (label, (queueWaitMs, 1L, queueWaitMs, queueWaitMs))
      }
      .reduceByKey { case ((s1, c1, mn1, mx1), (s2, c2, mn2, mx2)) =>
        (s1 + s2, c1 + c2, math.min(mn1, mn2), math.max(mx1, mx2))
      }

    println("\n[INFO] Queue wait time stats per status (ms):")
    println(f"  ${"Status"}%-10s  ${"Count"}%7s  ${"Avg(ms)"}%10s  ${"Min(ms)"}%10s  ${"Max(ms)"}%10s")
    println("  " + "-" * 55)
    val statsResults = queueStatsRDD.collect().sortBy(_._1)
    statsResults.foreach { case (label, (sum, count, mn, mx)) =>
      val avg = sum.toDouble / count
      println(f"  $label%-10s  $count%7d  $avg%10.1f  $mn%10d  $mx%10d")
    }

    // =========================================================
    // STEP 6: WRITE outputs to HDFS
    // =========================================================
    // 6a. Status counts
    val statusOutputRDD: RDD[String] = statusCountRDD.map { case (label, count) =>
      s"$label\t$count"
    }
    statusOutputRDD.coalesce(1).saveAsTextFile(OUTPUT_STATUS_PATH)
    println(s"\n[INFO] Status counts written to: $OUTPUT_STATUS_PATH")

    // 6b. Queue wait stats
    val statsOutputRDD: RDD[String] = queueStatsRDD.map { case (label, (sum, count, mn, mx)) =>
      val avg = sum.toDouble / count
      f"$label\t$count\t$avg%.1f\t$mn\t$mx"
    }
    statsOutputRDD.coalesce(1).saveAsTextFile(OUTPUT_QWAIT_PATH)
    println(s"[INFO] Queue stats written to: $OUTPUT_QWAIT_PATH")

    // =========================================================
    // STEP 7: TIMING REPORT
    // =========================================================
    val wallEnd     = System.currentTimeMillis()
    val elapsedMs   = wallEnd - wallStart
    val elapsedSec  = elapsedMs / 1000.0

    println("\n" + "=" * 65)
    println("TIMING REPORT -- Spark RDD (Phase 6)")
    println("=" * 65)
    println(f"  RDD Partitions   : $rawPartitions")
    println(f"  Total Records    : $totalParsed")
    println(f"  Elapsed (ms)     : $elapsedMs")
    println(f"  Elapsed (sec)    : $elapsedSec%.3f s")
    println("=" * 65)
    println("Record this in: metrics/spark_rdd_timing.txt")

    parsedRDD.unpersist()
    sc.stop()
  }
}
