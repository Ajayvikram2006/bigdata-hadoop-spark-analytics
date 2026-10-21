package edu.project1.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler

// VIVA NOTES - Logistic Regression
// * Binary classifier (SUCCESS vs FAILED) on features from /project/batch_results/sql_output. Baseline model: high accuracy but it catches almost no FAILED jobs
//   because the classes are imbalanced (see metrics/ml_lr_evaluation.txt).
// * The trained model is saved with .write.overwrite().save(...) to /project/models/logistic_regression and reloaded to prove persistence - a rerun overwrites it.
// * Run: spark-submit --class edu.project1.ml.CMULogisticRegression (docs/RUNBOOK.md section E6).
/**
 * CMULogisticRegression -- Phase 12 (Machine Learning Pipeline)
 *
 * Trains a Logistic Regression baseline model to predict job failures.
 * Reads the cleaned and feature-engineered Parquet data from previous phases.
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
 *     --class edu.project1.ml.CMULogisticRegression
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMULogisticRegression {

  val INPUT_PATH = "hdfs://namenode:9000/project/batch_results/sql_output"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMULogisticRegression-Phase12")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.dfs.replication", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 65)
    println("CMULogisticRegression -- Phase 12 -- Spark MLlib")
    println(s"Input Data: $INPUT_PATH")
    println("=" * 65)

    // =========================================================
    // STEP 1: LOAD FEATURE-ENGINEERED DATA
    // =========================================================
    // We use the SQL output from Phase 9 (can use Dataset/DataFrame output too, they are identical)
    val rawData = spark.read.parquet(INPUT_PATH)
    
    println(s"[INFO] Loaded ${rawData.count()} records.")

    // Ensure double types for numerical stability where necessary
    val data = rawData
      .withColumn("queueWaitMs", rawData("queueWaitMs").cast("double"))
      .withColumn("totalTasks", rawData("totalTasks").cast("double"))
      .withColumn("taskFailureCount", rawData("taskFailureCount").cast("double"))
      // label is already an integer (0 or 1), but MLlib often expects double
      .withColumn("label", rawData("label").cast("double"))

    // =========================================================
    // STEP 2: ASSEMBLE FEATURES VECTOR
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
      // handle invalid records just in case
      .setHandleInvalid("skip")

    val assembledData = assembler.transform(data).select("features", "label")

    // =========================================================
    // STEP 3: TRAIN / TEST SPLIT (80/20)
    // =========================================================
    val Array(trainingData, testData) = assembledData.randomSplit(Array(0.8, 0.2), seed = 1234L)
    
    println(s"[INFO] Training Data: ${trainingData.count()} records")
    println(s"[INFO] Testing Data : ${testData.count()} records")

    // =========================================================
    // STEP 4: TRAIN LOGISTIC REGRESSION MODEL
    // =========================================================
    // NOTE: Original regParam=0.3, elasticNetParam=0.8 was too strong —
    //       it drove all coefficients to zero (AUC=0.5000, predicts majority class).
    //       Tuned: regParam=0.01 (light L2), elasticNetParam=0.0, maxIter=100.
    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.01)
      .setElasticNetParam(0.0)   // Pure L2 regularization
      .setFeaturesCol("features")
      .setLabelCol("label")

    println("\n[INFO] Training Logistic Regression model...")
    val wallStart = System.currentTimeMillis()
    
    val lrModel = lr.fit(trainingData)
    
    val wallEnd = System.currentTimeMillis()
    println(s"[INFO] Training completed in ${(wallEnd - wallStart) / 1000.0} seconds.")

    // =========================================================
    // STEP 5: PREDICT ON TEST SET
    // =========================================================
    val predictions = lrModel.transform(testData)

    // =========================================================
    // STEP 6: EVALUATION METRICS
    // =========================================================
    // Accuracy
    val multiEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val accuracy  = multiEvaluator.setMetricName("accuracy").evaluate(predictions)
    val precision = multiEvaluator.setMetricName("weightedPrecision").evaluate(predictions)
    val recall    = multiEvaluator.setMetricName("weightedRecall").evaluate(predictions)
    val f1        = multiEvaluator.setMetricName("f1").evaluate(predictions)

    // AUC (Area Under ROC)
    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
      
    val auc = binaryEvaluator.evaluate(predictions)

    // =========================================================
    // CONFUSION MATRIX — most important for imbalanced datasets
    // For binary: label=0 (SUCCESS), label=1 (FAILED)
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
    println("EVALUATION RESULTS -- Logistic Regression (Phase 12)")
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
    println("Record this in: metrics/ml_lr_evaluation.txt")
    
    println("\n[INFO] Model Coefficients: ")
    featureCols.zip(lrModel.coefficients.toArray).foreach { case (feature, coef) =>
      println(f"  $feature%-20s: $coef%.6f")
    }
    println(f"  Intercept           : ${lrModel.intercept}%.6f")

    // =========================================================
    // STEP 7: PERSIST MODEL TO HDFS
    // =========================================================
    val MODEL_SAVE_PATH = "hdfs://namenode:9000/project/models/logistic_regression"
    println(s"\n[INFO] Saving Logistic Regression model to: $MODEL_SAVE_PATH")
    lrModel.write.overwrite().save(MODEL_SAVE_PATH)
    println("[INFO] Model saved successfully.")

    // Verify: reload the model and make a test prediction
    println("[INFO] Verifying model reload...")
    val reloadedLR = org.apache.spark.ml.classification.LogisticRegressionModel.load(MODEL_SAVE_PATH)
    val samplePred = reloadedLR.transform(testData.limit(1))
    println(s"[INFO] Reload verification PASSED — reloaded model can make predictions.")

    spark.stop()
  }
}
