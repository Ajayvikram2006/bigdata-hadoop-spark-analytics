package edu.project1.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * CMUStreamingJob -- Phase 10 (Spark Streaming)
 *
 * Demonstrates Spark Structured Streaming capabilities.
 * Simulates a real-time data feed using a socket stream (Netcat).
 * Parses comma-separated job records and maintains a running 
 * aggregation of job status counts.
 *
 * Usage:
 *   1. Start a netcat server in one terminal:
 *      docker exec -it spark-master nc -lk 9999
 *   2. Run this job in another terminal:
 *      docker exec -it spark-master /spark/bin/spark-submit \
 *        --master local[2] \
 *        --class edu.project1.streaming.CMUStreamingJob \
 *        /opt/spark-apps/bigdata-analytics-assembly-1.0.0.jar
 *   3. Type comma-separated values into the netcat terminal:
 *      1234,999,1000,1050,0,10,2
 *      1235,998,1000,1050,1,10,2
 */
object CMUStreamingJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMUStreamingJob-Phase10")
      // Using local[2] for streaming because it needs at least 2 cores (1 for receiver, 1 for processing)
      .config("spark.master", "local[2]") 
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("=" * 65)
    println("CMUStreamingJob -- Phase 10 -- Spark Structured Streaming")
    println("Waiting for streaming data on spark-master:9999...")
    println("=" * 65)

    // =========================================================
    // STEP 1: READ STREAM FROM SOCKET
    // =========================================================
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // =========================================================
    // STEP 2: PARSE CSV LINES & EXTRACT STATUS
    // Assuming CSV format: jtid,jobid,submitTime,launchTime,status,numMaps,numReduces
    // =========================================================
    val parsedData = lines.select(
      split(col("value"), ",").getItem(4).cast(IntegerType).as("statusCode")
    ).filter(col("statusCode").isNotNull)

    val labeledData = parsedData.withColumn("status", 
      when(col("statusCode") === 0, "SUCCESS")
      .when(col("statusCode") === 1, "FAILED")
      .when(col("statusCode") === 2, "KILLED")
      .otherwise("UNKNOWN")
    )

    // =========================================================
    // STEP 3: AGGREGATE
    // =========================================================
    val statusCounts = labeledData.groupBy("status").count()

    // =========================================================
    // STEP 4: WRITE STREAM TO CONSOLE (Complete Mode for aggregates)
    // =========================================================
    val query = statusCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}
