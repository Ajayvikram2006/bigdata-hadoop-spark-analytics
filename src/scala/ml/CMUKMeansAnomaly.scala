package edu.project1.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}

/**
 * CMUKMeansAnomaly -- Phase 14.2 (Anomaly Detection)
 *
 * Implements unsupervised anomaly detection for the HDFS_v2 dataset.
 * Since HDFS_v2 has no ground-truth labels, we use K-Means clustering.
 *
 * Pipeline:
 * 1. Reads parsed HDFS logs.
 * 2. Groups logs into 5-minute tumbling windows per node.
 * 3. Extracts features: eventCount, errorCount, warningCount, uniqueComponents.
 * 4. Scales features using StandardScaler (crucial for distance-based K-Means).
 * 5. Trains K-Means clustering model (k=5).
 * 6. Calculates anomaly score (Euclidean distance to assigned centroid).
 * 7. Flags top most distant windows as ANOMALIES.
 *
 * Usage (inside spark-master container):
 *   /spark/bin/spark-submit --master spark://spark-master:7077
 *     --class edu.project1.ml.CMUKMeansAnomaly
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMUKMeansAnomaly {

  val INPUT_PATH = "hdfs://namenode:9000/project/processed/hdfs_logs"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMUKMeansAnomaly-Phase14.2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 70)
    println("CMUKMeansAnomaly -- Phase 14.2 -- Unsupervised Learning")
    println(s"Input Data: $INPUT_PATH")
    println("=" * 70)

    import spark.implicits._

    // =========================================================
    // STEP 1: LOAD PARSED LOGS
    // =========================================================
    val rawLogs = spark.read.parquet(INPUT_PATH)
    
    // =========================================================
    // STEP 2: TEMPORAL FEATURE ENGINEERING (5-min windows)
    // =========================================================
    println("\n[INFO] Extracting time-windowed features (5 minutes)...")
    val windowedFeatures = rawLogs
      .groupBy(
        window(col("log_ts"), "5 minutes"),
        col("node_name")
      )
      .agg(
        count("*").cast("double").as("eventCount"),
        sum(when(col("level") === "ERROR", 1).otherwise(0)).cast("double").as("errorCount"),
        sum(when(col("level") === "WARN", 1).otherwise(0)).cast("double").as("warningCount"),
        countDistinct("component").cast("double").as("uniqueComponents")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("node_name"),
        col("eventCount"),
        col("errorCount"),
        col("warningCount"),
        col("uniqueComponents")
      )

    // Cache because we will use it for scaling, training, and predicting
    windowedFeatures.cache()
    val totalWindows = windowedFeatures.count()
    println(s"[INFO] Total 5-minute node windows generated: $totalWindows")

    // =========================================================
    // STEP 3: ASSEMBLE & SCALE FEATURES
    // =========================================================
    val featureCols = Array("eventCount", "errorCount", "warningCount", "uniqueComponents")
    
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("raw_features")

    val assembledData = assembler.transform(windowedFeatures)

    // K-Means uses Euclidean distance, so scaling is mathematically required
    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(assembledData)
    val scaledData = scalerModel.transform(assembledData)

    // =========================================================
    // STEP 4: TRAIN K-MEANS MODEL
    // =========================================================
    val k = 5
    println(s"\n[INFO] Training K-Means clustering model (k=$k)...")
    val wallStart = System.currentTimeMillis()

    val kmeans = new KMeans()
      .setK(k)
      .setSeed(1234L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val kmeansModel = kmeans.fit(scaledData)
    
    val wallEnd = System.currentTimeMillis()
    println(s"[INFO] Training completed in ${(wallEnd - wallStart) / 1000.0} seconds.")

    // =========================================================
    // STEP 5: COMPUTE ANOMALY SCORES
    // =========================================================
    println("\n[INFO] Calculating anomaly scores (distance to centroid)...")
    
    val centers = kmeansModel.clusterCenters
    
    // Broadcast cluster centers to worker nodes
    val broadcastCenters = spark.sparkContext.broadcast(centers)

    // UDF to calculate Euclidean distance between a feature vector and its assigned cluster centroid
    val calculateDistance = udf((clusterId: Int, features: MLVector) => {
      val centroid = broadcastCenters.value(clusterId)
      Vectors.sqdist(features, centroid) // squared euclidean distance
    })

    val scoredData = kmeansModel.transform(scaledData)
      .withColumn("anomaly_score", calculateDistance(col("cluster"), col("features")))

    // =========================================================
    // STEP 6: IDENTIFY & OUTPUT ANOMALIES
    // =========================================================
    println("\n" + "=" * 70)
    println("ANOMALY DETECTION RESULTS -- K-Means (Phase 14.2)")
    println("=" * 70)
    
    // Print cluster sizes
    println("[INFO] Cluster distribution:")
    scoredData.groupBy("cluster").count().orderBy("cluster").show()

    // Threshold for anomalies: Top 0.1% or a fixed arbitrary high distance.
    // We will just show the top 10 most anomalous windows globally.
    println("[INFO] Top 10 Most Anomalous Node Windows (Highest Anomaly Score):")
    scoredData
      .select("window_start", "node_name", "anomaly_score", "eventCount", "errorCount", "warningCount", "cluster")
      .orderBy(desc("anomaly_score"))
      .show(10, truncate = false)
      
    println("======================================================================")
    println("Record this in: metrics/ml_kmeans_anomaly.txt")

    spark.stop()
  }
}
