package edu.project1.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

// VIVA NOTES - Drift detection
// * Chronological split: first 70% of records = reference distribution, last 30% = current. For each feature the mean shift is compared in standard-deviation units (z-score);
//   z > 1.0 would raise a MODEL DRIFT WARNING. Saved result: all STABLE (metrics/ml_drift_detection.txt). Read-only on HDFS. Run: docs/RUNBOOK.md section E9.
/**
 * CMUDriftDetection -- Phase 16
 *
 * Detects concept/data drift in the ML features.
 * Splits the CMU job dataset chronologically into:
 *   - Reference (Training/Baseline): e.g. 2010
 *   - Current (Incoming/New Data): e.g. 2011
 *
 * It calculates the Mean and StdDev of key features (e.g. totalTasks)
 * and flags a MODEL DRIFT WARNING if the current mean deviates
 * by more than 1.5 standard deviations from the reference baseline
 * (Z-score based statistical drift detection).
 *
 * Usage (inside spark-master container):
 *   /spark/bin/spark-submit --master spark://spark-master:7077
 *     --class edu.project1.ml.CMUDriftDetection
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMUDriftDetection {

  val INPUT_PATH = "hdfs://namenode:9000/project/batch_results/sql_output"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMUDriftDetection-Phase16")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 80)
    println("CMUDriftDetection -- Phase 16 -- Statistical Drift Detection")
    println(s"Input Data: $INPUT_PATH")
    println("=" * 80)

    val rawData = spark.read.parquet(INPUT_PATH)
    
    // Create a pseudo-time split (since we don't have true streaming CMU data, 
    // we simulate time by splitting the dataset into two halves based on submitTime)
    val sortedData = rawData.orderBy("jobid")
    val totalCount = sortedData.count()
    val midPoint = (totalCount * 0.7).toLong // 70% ref, 30% current

    // Use RDD zipWithIndex to split deterministically
    val rddWithIndex = sortedData.rdd.zipWithIndex()
    
    // Reconstruct DataFrames
    val refSchema = sortedData.schema
    val refDF = spark.createDataFrame(
      rddWithIndex.filter(_._2 < midPoint).map(_._1),
      refSchema
    )
    
    val curDF = spark.createDataFrame(
      rddWithIndex.filter(_._2 >= midPoint).map(_._1),
      refSchema
    )

    println(s"[INFO] Reference Distribution (Baseline) : ${refDF.count()} records")
    println(s"[INFO] Current Distribution (Incoming)   : ${curDF.count()} records")
    println("-" * 80)

    val featuresToMonitor = Array("totalTasks", "mapReduceRatio", "queueWaitMs")

    println(f"${"FEATURE"}%-18s | ${"REF MEAN"}%-10s | ${"CUR MEAN"}%-10s | ${"Z-SCORE"}%-8s | ${"STATUS"}")
    println("-" * 80)

    featuresToMonitor.foreach { feature =>
      // 1. Calculate Reference Statistics
      val refStats = refDF.select(
        mean(col(feature).cast(DoubleType)).as("ref_mean"),
        stddev(col(feature).cast(DoubleType)).as("ref_std")
      ).head()
      
      val refMean = refStats.getAs[Double]("ref_mean")
      val refStd = refStats.getAs[Double]("ref_std")

      // 2. Calculate Current Statistics
      val curStats = curDF.select(
        mean(col(feature).cast(DoubleType)).as("cur_mean")
      ).head()
      
      val curMean = curStats.getAs[Double]("cur_mean")

      // 3. Compute Z-Score for the drift
      // Z = |cur_mean - ref_mean| / ref_std
      val zScore = Math.abs(curMean - refMean) / (if (refStd == 0) 1e-9 else refStd)

      // 4. Alert Threshold
      val isDrifting = zScore > 1.0 // Trigger if deviates by more than 1 std deviation
      val status = if (isDrifting) "🚨 MODEL DRIFT WARNING" else "✅ STABLE"

      println(f"$feature%-18s | $refMean%10.2f | $curMean%10.2f | $zScore%8.2f | $status")
    }

    println("=" * 80)
    println("Record this in: metrics/ml_drift_detection.txt")

    spark.stop()
  }
}
