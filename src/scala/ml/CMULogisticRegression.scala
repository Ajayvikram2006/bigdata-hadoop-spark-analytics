package edu.project1.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler

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
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
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
    // Accuracy Evaluator
    val multiEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = multiEvaluator.evaluate(predictions)

    // AUC Evaluator (Area Under ROC)
    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
      
    val auc = binaryEvaluator.evaluate(predictions)

    println("\n" + "=" * 65)
    println("EVALUATION RESULTS -- Logistic Regression (Phase 12)")
    println("=" * 65)
    println(f"  Accuracy       : $accuracy%.4f")
    println(f"  Area Under ROC : $auc%.4f")
    println("=" * 65)
    println("Record this in: metrics/ml_lr_evaluation.txt")
    
    println("\n[INFO] Model Coefficients: ")
    featureCols.zip(lrModel.coefficients.toArray).foreach { case (feature, coef) =>
      println(f"  $feature%-20s: $coef%.6f")
    }
    println(f"  Intercept           : ${lrModel.intercept}%.6f")

    spark.stop()
  }
}
