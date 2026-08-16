package edu.project1.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler

/**
 * CMURandomForest -- Phase 13 (Machine Learning Pipeline)
 *
 * Trains a Random Forest primary model to predict job failures.
 * This model can capture non-linear relationships and interactions 
 * between features, significantly outperforming the LR baseline.
 * 
 * Features used:
 * - queueWaitMs
 * - totalTasks
 * - mapReduceRatio
 * - taskFailureCount
 * - retryRate
 *
 * Label: status (0=SUCCESS, 1=FAILED)
 *
 * Usage (inside spark-master container):
 *   /spark/bin/spark-submit --master spark://spark-master:7077
 *     --class edu.project1.ml.CMURandomForest
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMURandomForest {

  val INPUT_PATH = "hdfs://namenode:9000/project/batch_results/sql_output"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMURandomForest-Phase13")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 65)
    println("CMURandomForest -- Phase 13 -- Spark MLlib")
    println(s"Input Data: $INPUT_PATH")
    println("=" * 65)

    // =========================================================
    // STEP 1: LOAD DATA
    // =========================================================
    val rawData = spark.read.parquet(INPUT_PATH)
    
    println(s"[INFO] Loaded ${rawData.count()} records.")

    val data = rawData
      .withColumn("queueWaitMs", rawData("queueWaitMs").cast("double"))
      .withColumn("totalTasks", rawData("totalTasks").cast("double"))
      .withColumn("taskFailureCount", rawData("taskFailureCount").cast("double"))
      .withColumn("label", rawData("label").cast("double"))

    // =========================================================
    // STEP 2: ASSEMBLE FEATURES
    // =========================================================
    val featureCols = Array(
      "queueWaitMs", 
      "totalTasks", 
      "mapReduceRatio", 
      "taskFailureCount", 
      "retryRate"
    )
    
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val assembledData = assembler.transform(data).select("features", "label")

    // =========================================================
    // STEP 3: TRAIN / TEST SPLIT (80/20) - SAME SEED AS LR
    // =========================================================
    val Array(trainingData, testData) = assembledData.randomSplit(Array(0.8, 0.2), seed = 1234L)
    
    println(s"[INFO] Training Data: ${trainingData.count()} records")
    println(s"[INFO] Testing Data : ${testData.count()} records")

    // =========================================================
    // STEP 4: TRAIN RANDOM FOREST MODEL
    // =========================================================
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .setMaxDepth(10)
      .setSeed(42L)

    println("\n[INFO] Training Random Forest model...")
    val wallStart = System.currentTimeMillis()
    
    val rfModel = rf.fit(trainingData)
    
    val wallEnd = System.currentTimeMillis()
    println(s"[INFO] Training completed in ${(wallEnd - wallStart) / 1000.0} seconds.")

    // =========================================================
    // STEP 5: PREDICT & EVALUATE
    // =========================================================
    val predictions = rfModel.transform(testData)

    val multiEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = multiEvaluator.evaluate(predictions)

    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
      
    val auc = binaryEvaluator.evaluate(predictions)

    println("\n" + "=" * 65)
    println("EVALUATION RESULTS -- Random Forest (Phase 13)")
    println("=" * 65)
    println(f"  Accuracy       : $accuracy%.4f")
    println(f"  Area Under ROC : $auc%.4f")
    println("=" * 65)
    println("Record this in: metrics/ml_rf_evaluation.txt")
    
    println("\n[INFO] Feature Importances: ")
    val importances = rfModel.featureImportances.toArray
    featureCols.zip(importances).sortBy(-_._2).foreach { case (feature, importance) =>
      println(f"  $feature%-20s: $importance%.6f")
    }

    spark.stop()
  }
}
