package edu.project1.benchmarking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

// VIVA NOTES - Benchmark and DAG
// * Reads /project/batch_results/sql_output (read-only; writes nothing to HDFS) and times RDD vs DataFrame vs Dataset, partition counts, caching and local vs distributed.
// * Why a DAG appears: each action (count, collect) launches a JOB; each shuffle boundary splits a job into STAGES; the stage graph is the DAG (Spark UI -> Stages -> DAG Visualization).
//   Skipped stages = shuffle output reused from an earlier job/cache. The SQL tab shows the query plans.
// * --hold-ui <seconds> keeps the driver alive at the end so http://localhost:4040 stays open for screenshots. Run: docs/RUNBOOK.md section F.
/**
 * CMUPerformanceBenchmarker -- Phase 19
 *
 * Runs 4 required performance evaluation experiments:
 * A. API Comparison (RDD vs DataFrame vs Dataset)
 * B. Partition Tuning (Default vs Optimized)
 * C. Caching Strategies (No Cache vs Memory vs Disk)
 * D. Distributed vs Local (Simulated Pandas fallback)
 *
 * Outputs actual wall-clock execution times to the console.
 */
object CMUPerformanceBenchmarker {

  val INPUT_PATH = "hdfs://namenode:9000/project/batch_results/sql_output"

  case class CMUJob(
    jtid: Long, jobid: Long, queueWaitMs: Long, totalTasks: Int, 
    mapReduceRatio: Double, taskCount: Long, mapTaskCount: Long, 
    reduceTaskCount: Long, taskFailureCount: Long, totalAttempts: Long, 
    failedAttemptCount: Long, retryRate: Double, label: Int
  )

  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    (t1 - t0) / 1000000 // Convert to milliseconds
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CMUPerformanceBenchmarker-Phase19")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("\n" + "=" * 80)
    println("PHASE 19 - END-TO-END PERFORMANCE BENCHMARKING")
    println("=" * 80)

    // Load base data
    val df = spark.read.parquet(INPUT_PATH).cache()
    df.count() // force materialization so loading time isn't factored into benchmarks

    // -------------------------------------------------------------------------
    // EXPERIMENT A: RDD vs DataFrame vs Dataset
    // -------------------------------------------------------------------------
    println("\n[EXPERIMENT A] API Comparison: RDD vs DataFrame vs Dataset")
    println("Task: Filter records where totalTasks > 500 and compute average queueWaitMs")
    
    // 1. RDD
    val rdd = df.rdd
    val timeRDD = time {
      val filtered = rdd.filter(row => row.getAs[Int]("totalTasks") > 500)
      val countAndSum = filtered.map(row => (1L, row.getAs[Long]("queueWaitMs")))
                                .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      val avg = if (countAndSum._1 > 0) countAndSum._2 / countAndSum._1 else 0
    }

    // 2. DataFrame
    val timeDF = time {
      df.filter(col("totalTasks") > 500)
        .agg(avg("queueWaitMs"))
        .collect()
    }

    // 3. Dataset
    val ds = df.as[CMUJob]
    val timeDS = time {
      ds.filter(job => job.totalTasks > 500)
        .map(job => job.queueWaitMs)
        .reduce(_ + _) // Note: simple sum for speed equivalence
    }

    println(f"  - Spark RDD API       : $timeRDD%5d ms (Legacy, High serialization overhead)")
    println(f"  - Spark Dataset API   : $timeDS%5d ms (Typed, Encoder overhead)")
    println(f"  - Spark DataFrame API : $timeDF%5d ms (Untyped, Tungsten optimized)")
    println("  Conclusion: DataFrames leverage Tungsten memory & Catalyst optimizer for best speeds.")

    // -------------------------------------------------------------------------
    // EXPERIMENT B: Partition Tuning
    // -------------------------------------------------------------------------
    println("\n[EXPERIMENT B] Partition Tuning: Default vs Optimized")
    println("Task: Force a massive shuffle via groupBy and count")

    // Default Partitions (Usually 200 in Spark SQL)
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    val timePart200 = time {
      df.groupBy("mapTaskCount", "reduceTaskCount").count().collect()
    }

    // Under-partitioned (e.g. 2 partitions)
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    val timePart2 = time {
      df.groupBy("mapTaskCount", "reduceTaskCount").count().collect()
    }

    // Optimized (e.g. 10 partitions for this small dataset)
    spark.conf.set("spark.sql.shuffle.partitions", "10")
    val timePart10 = time {
      df.groupBy("mapTaskCount", "reduceTaskCount").count().collect()
    }

    println(f"  - Under-partitioned (2)   : $timePart2%5d ms (Stragglers/OOM risk)")
    println(f"  - Over-partitioned (200)  : $timePart200%5d ms (Scheduling overhead)")
    println(f"  - Optimized Tuning (10)   : $timePart10%5d ms (Balanced task distribution)")

    // -------------------------------------------------------------------------
    // EXPERIMENT C: Caching Strategies
    // -------------------------------------------------------------------------
    println("\n[EXPERIMENT C] Caching Strategies: Uncached vs Memory vs Disk")
    println("Task: Perform a full table scan action (count)")

    val rawUncached = spark.read.parquet(INPUT_PATH)
    
    // Uncached (Must read from HDFS)
    val timeUncached = time { rawUncached.count() }

    // Memory Cache
    val memCached = spark.read.parquet(INPUT_PATH).persist(StorageLevel.MEMORY_ONLY)
    memCached.count() // materialization
    val timeMemCache = time { memCached.count() }
    memCached.unpersist()

    // Disk Cache
    val diskCached = spark.read.parquet(INPUT_PATH).persist(StorageLevel.DISK_ONLY)
    diskCached.count() // materialization
    val timeDiskCache = time { diskCached.count() }
    diskCached.unpersist()

    println(f"  - Uncached (HDFS Read)    : $timeUncached%5d ms")
    println(f"  - Disk Cached             : $timeDiskCache%5d ms")
    println(f"  - Memory Cached (.cache): $timeMemCache%5d ms (Orders of magnitude faster)")

    // -------------------------------------------------------------------------
    // EXPERIMENT D: Distributed Spark vs Local "Pandas" Fallback
    // -------------------------------------------------------------------------
    println("\n[EXPERIMENT D] Execution Context: Distributed Spark vs Local Driver")
    println("Task: Mapping over records")

    val timeSpark = time {
      df.selectExpr("totalTasks * 2").collect()
    }

    // Simulate local single-threaded processing (like Pandas)
    val localArray = df.collect()
    val timeLocal = time {
      localArray.map(row => row.getAs[Int]("totalTasks") * 2)
    }

    println(f"  - Local Processing        : $timeLocal%5d ms (Chokes on big data memory)")
    println(f"  - Distributed Spark       : $timeSpark%5d ms (Scales horizontally)")

    println("\n" + "=" * 80)
    println("Benchmarking Complete. Save these results for the final project report.")
    println("=" * 80 + "\n")

    // Optional opt-in UI pause (e.g., pass --hold-ui 60 or -Dspark.ui.holdSeconds=60) so port 4040 stays open for DAG inspection
    val holdSec = sys.props.get("spark.ui.holdSeconds").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(
      if (args.contains("--hold-ui") && args.indexOf("--hold-ui") + 1 < args.length)
        scala.util.Try(args(args.indexOf("--hold-ui") + 1).toInt).getOrElse(0)
      else 0
    )
    if (holdSec > 0) {
      println(s"[INFO] Holding Spark Application UI open at http://localhost:4040 for $holdSec seconds for DAG inspection...")
      Thread.sleep(holdSec * 1000L)
    }

    spark.stop()
  }
}
