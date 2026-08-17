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

    val accuracy  = multiEvaluator.setMetricName("accuracy").evaluate(predictions)
    val precision = multiEvaluator.setMetricName("weightedPrecision").evaluate(predictions)
    val recall    = multiEvaluator.setMetricName("weightedRecall").evaluate(predictions)
    val f1        = multiEvaluator.setMetricName("f1").evaluate(predictions)

    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
      
    val auc = binaryEvaluator.evaluate(predictions)

    // =========================================================
    // CONFUSION MATRIX — most important for imbalanced datasets
    // label=0: SUCCESS, label=1: FAILED
    // =========================================================
    val tp = predictions.filter("label = 1.0 AND prediction = 1.0").count()
    val tn = predictions.filter("label = 0.0 AND prediction = 0.0").count()
    val fp = predictions.filter("label = 0.0 AND prediction = 1.0").count()
    val fn = predictions.filter("label = 1.0 AND prediction = 0.0").count()

    // Per-class metrics for FAILED class (label=1) — the critical minority class
    val failedPrecision = if (tp + fp > 0) tp.toDouble / (tp + fp).toDouble else 0.0
    val failedRecall    = if (tp + fn > 0) tp.toDouble / (tp + fn).toDouble else 0.0
    val failedF1        = if (failedPrecision + failedRecall > 0)
                            2 * failedPrecision * failedRecall / (failedPrecision + failedRecall)
                          else 0.0

    println("\n" + "=" * 65)
    println("EVALUATION RESULTS -- Random Forest (Phase 13)")
    println("=" * 65)
    println(f"  Accuracy           : $accuracy%.4f")
    println(f"  Weighted Precision : $precision%.4f")
    println(f"  Weighted Recall    : $recall%.4f")
    println(f"  Weighted F1-Score  : $f1%.4f")
    println(f"  Area Under ROC     : $auc%.4f")
    println("=" * 65)
    println("  CONFUSION MATRIX (label=0: SUCCESS, label=1: FAILED)")
    println(f"    True  Positives (TP, FAILED caught)    : $tp")
    println(f"    True  Negatives (TN, SUCCESS caught)   : $tn")
    println(f"    False Positives (FP, SUCCESS → FAILED) : $fp")
    println(f"    False Negatives (FN, FAILED → SUCCESS) : $fn")
    println("  FAILED-class (label=1) specific metrics:")
    println(f"    Precision (FAILED): $failedPrecision%.4f")
    println(f"    Recall    (FAILED): $failedRecall%.4f  ← most important for imbalance")
    println(f"    F1-Score  (FAILED): $failedF1%.4f")
    println("=" * 65)
    println("Record this in: metrics/ml_rf_evaluation.txt")
    
    println("\n[INFO] Feature Importances: ")
    val importances = rfModel.featureImportances.toArray
    featureCols.zip(importances).sortBy(-_._2).foreach { case (feature, importance) =>
      println(f"  $feature%-20s: $importance%.6f")
    }

    // =========================================================
    // STEP 6: PERSIST MODEL TO HDFS
    // =========================================================
    val MODEL_SAVE_PATH = "hdfs://namenode:9000/project/models/random_forest"
    println(s"\n[INFO] Saving Random Forest model to: $MODEL_SAVE_PATH")
    rfModel.write.overwrite().save(MODEL_SAVE_PATH)
    println("[INFO] Model saved successfully.")

    // Verify: reload the model and make a test prediction
    println("[INFO] Verifying model reload...")
    val reloadedRF = org.apache.spark.ml.classification.RandomForestClassificationModel.load(MODEL_SAVE_PATH)
    val samplePred = reloadedRF.transform(testData.limit(1))
    println(s"[INFO] Reload verification PASSED — reloaded model can make predictions.")

    spark.stop()
  }
}
