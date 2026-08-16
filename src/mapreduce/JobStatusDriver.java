package edu.project1.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * JobStatusDriver — Phase 5 (MapReduce Baseline)
 *
 * Orchestrates the MapReduce job that counts CMU job statuses.
 *
 * Execution (inside namenode container):
 *   hadoop jar /opt/hadoop-apps/job-status-mr.jar \
 *     edu.project1.mapreduce.JobStatusDriver \
 *     /project/raw/cmu/*/job.csv \
 *     /project/mapreduce_output/job_status_counts
 *
 * The driver:
 *   1. Validates input/output paths
 *   2. Cleans any prior output directory
 *   3. Configures job with 1 Reducer (single output file for easy reading)
 *   4. Times the full execution
 *   5. Prints timing summary to stdout for metrics capture
 *
 * Timing output is recorded in: metrics/mapreduce_timing.txt
 */
public class JobStatusDriver extends Configured implements Tool {

    // HDFS paths (overridable via CLI args)
    private static final String DEFAULT_INPUT  =
            "hdfs://namenode:9000/project/raw/cmu/*/job.csv";
    private static final String DEFAULT_OUTPUT =
            "hdfs://namenode:9000/project/mapreduce_output/job_status_counts";

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        // Resolve input / output paths from CLI or defaults
        String inputPath  = (args.length > 0) ? args[0] : DEFAULT_INPUT;
        String outputPath = (args.length > 1) ? args[1] : DEFAULT_OUTPUT;

        System.out.println("=".repeat(65));
        System.out.println("JobStatusDriver — Phase 5 — MapReduce Baseline");
        System.out.println("Input  : " + inputPath);
        System.out.println("Output : " + outputPath);
        System.out.println("=".repeat(65));

        // ── Clean previous output if it exists ──────────────
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outputPath);
        if (fs.exists(outPath)) {
            System.out.println("[WARN] Output path exists — deleting: " + outputPath);
            fs.delete(outPath, true);
        }

        // ── Configure MapReduce Job ─────────────────────────
        Job job = Job.getInstance(conf, "CMU-JobStatus-Counter");
        job.setJarByClass(JobStatusDriver.class);

        // Mapper and Reducer classes
        job.setMapperClass(JobStatusMapper.class);
        job.setReducerClass(JobStatusReducer.class);

        // Combiner: use the Reducer as a local combiner (pure sum — safe)
        job.setCombinerClass(JobStatusReducer.class);

        // Output key/value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input/Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 1 reducer → single output file, easy faculty demonstration
        job.setNumReduceTasks(1);

        // Set I/O paths (supports glob pattern for */job.csv)
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, outPath);

        // ── Execute and Time ────────────────────────────────
        long startMs = System.currentTimeMillis();
        System.out.println("[INFO] Job submitted at: " + new java.util.Date(startMs));

        boolean success = job.waitForCompletion(true);

        long endMs   = System.currentTimeMillis();
        long elapsedMs = endMs - startMs;
        long elapsedSec = elapsedMs / 1000;

        // ── Timing Report ───────────────────────────────────
        System.out.println("\n" + "=".repeat(65));
        System.out.println("TIMING REPORT — MapReduce Baseline");
        System.out.println("=".repeat(65));
        System.out.printf("  Start time   : %s%n", new java.util.Date(startMs));
        System.out.printf("  End time     : %s%n", new java.util.Date(endMs));
        System.out.printf("  Elapsed time : %d ms  (%d seconds)%n", elapsedMs, elapsedSec);
        System.out.printf("  Status       : %s%n", success ? "SUCCESS" : "FAILED");
        System.out.println("=".repeat(65));
        System.out.println("Record this in: metrics/mapreduce_timing.txt");

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new JobStatusDriver(), args);
        System.exit(exitCode);
    }
}
