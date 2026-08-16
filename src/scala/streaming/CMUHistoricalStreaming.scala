package edu.project1.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/**
 * CMUHistoricalStreaming -- Phase 15.1
 *
 * "Historical log replay via Structured Streaming"
 * 
 * Reads the raw HDFS_v2 log directory chronologically as a streaming source.
 * Parses the lines, aggregates them into 5-minute tumbling windows, 
 * performs threshold-based anomaly detection (learned from K-Means Phase 14),
 * and generates alerts when anomalies are detected.
 *
 * Usage (inside spark-master container):
 *   /spark/bin/spark-submit --master spark://spark-master:7077
 *     --class edu.project1.streaming.CMUHistoricalStreaming
 *     /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 */
object CMUHistoricalStreaming {

  val STREAM_INPUT_PATH  = "hdfs://namenode:9000/project/raw/hdfs_v2"
  val STREAM_OUTPUT_PATH = "hdfs://namenode:9000/project/streaming_output"
  val CHECKPOINT_PATH    = "hdfs://namenode:9000/project/checkpoints/historical_stream"

  val LOG_PATTERN = """^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+(\w+)\s+([\w\.]+):\s+(.*)$"""

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMUHistoricalStreaming-Phase15")
      .config("spark.sql.shuffle.partitions", "10")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 70)
    println("Historical log replay via Structured Streaming -- Phase 15")
    println(s"Source: $STREAM_INPUT_PATH")
    println("=" * 70)

    import spark.implicits._

    // 1. Read Stream
    val rawStream = spark.readStream
      .option("maxFilesPerTrigger", 1) // Replay history slowly (1 file per micro-batch)
      .text(STREAM_INPUT_PATH)

    // 2. Parse & Structure
    val parsedStream = rawStream
      .withColumn("matched", regexp_extract(col("value"), LOG_PATTERN, 0) =!= "")
      .filter(col("matched") === true)
      .withColumn("log_ts_str", regexp_extract(col("value"), LOG_PATTERN, 1))
      .withColumn("level",      regexp_extract(col("value"), LOG_PATTERN, 2))
      .withColumn("node_name",  regexp_extract(input_file_name(), "(mesos-\\d+|namenode|secondarynamenode)", 1))
      .withColumn("log_ts",     to_timestamp(regexp_replace(col("log_ts_str"), ",", "."), "yyyy-MM-dd HH:mm:ss.SSS"))
      .filter(col("log_ts").isNotNull)
      // Watermark is required for stateful window aggregations in Structured Streaming
      .withWatermark("log_ts", "10 minutes")

    // 3. Windowed Aggregation (5 minutes)
    val windowedStream = parsedStream
      .groupBy(
        window(col("log_ts"), "5 minutes"),
        col("node_name")
      )
      .agg(
        count("*").as("eventCount"),
        sum(when(col("level") === "ERROR", 1).otherwise(0)).as("errorCount"),
        sum(when(col("level") === "WARN", 1).otherwise(0)).as("warningCount")
      )

    // 4. Anomaly Detection (Thresholds derived from Phase 14 K-Means analysis)
    // We discovered that > 50,000 events in a 5 min window is highly anomalous (Metadata Storm)
    val anomalyStream = windowedStream
      .withColumn("is_anomaly", 
        when(col("eventCount") > 50000 || col("errorCount") > 1000, true).otherwise(false)
      )
      .filter(col("is_anomaly") === true)

    // 5. Output Sink (Console for demonstration/screenshot)
    val query = anomalyStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 20)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    println("[INFO] Streaming query started. Waiting for anomalies to be detected...")
    println("[INFO] (Press Ctrl+C to terminate after capturing screenshot SS-28)")

    // Also write to HDFS (using a memory sink or file sink in a real production environment, 
    // but console is required for SS-28 verification).
    
    query.awaitTermination()
  }
}
