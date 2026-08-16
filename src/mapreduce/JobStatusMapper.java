package edu.project1.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * JobStatusMapper — Phase 5 (MapReduce Baseline)
 *
 * INPUT:  Raw CMU job.csv lines from HDFS
 *         Format: jtid,jobid,submitTime,launchTime,finishTime,status,numMaps,numReduces,...
 *
 * MAP:    For each valid data row, emit:
 *         KEY   = human-readable status label (SUCCESS / FAILED / KILLED / UNKNOWN_<n>)
 *         VALUE = IntWritable(1)
 *
 * SKIPS:  Header lines (starting with "jtid")
 *         Malformed lines (fewer than 6 fields)
 *         SASL/INFO log lines emitted by HDFS client
 */
public class JobStatusMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Column index for status field (0-indexed)
    private static final int STATUS_COL = 5;

    // Status codes from CMU dataset spec
    private static final int STATUS_SUCCESS = 0;
    private static final int STATUS_FAILED  = 1;
    private static final int STATUS_KILLED  = 2;

    // Reusable Writable objects (avoid GC pressure in tight loops)
    private final IntWritable ONE   = new IntWritable(1);
    private final Text        label = new Text();

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        String raw = line.toString().trim();

        // Skip blank lines
        if (raw.isEmpty()) return;

        // Skip CSV header row
        if (raw.startsWith("jtid")) return;

        // Skip HDFS SASL / INFO log lines (begin with year like "2026-")
        if (raw.startsWith("20") && raw.contains("INFO")) return;

        // Parse CSV
        String[] fields = raw.split(",", -1);
        if (fields.length < STATUS_COL + 1) {
            // Malformed record — count it separately for QA
            context.getCounter("DataQuality", "MALFORMED_RECORDS").increment(1);
            return;
        }

        // Parse status field
        int statusCode;
        try {
            statusCode = Integer.parseInt(fields[STATUS_COL].trim());
        } catch (NumberFormatException e) {
            context.getCounter("DataQuality", "UNPARSEABLE_STATUS").increment(1);
            return;
        }

        // Map status code to human-readable label
        switch (statusCode) {
            case STATUS_SUCCESS:
                label.set("SUCCESS");
                break;
            case STATUS_FAILED:
                label.set("FAILED");
                break;
            case STATUS_KILLED:
                label.set("KILLED");
                break;
            default:
                label.set("UNKNOWN_" + statusCode);
                context.getCounter("DataQuality", "UNKNOWN_STATUS").increment(1);
                break;
        }

        context.write(label, ONE);
        context.getCounter("JobRecords", "TOTAL_PROCESSED").increment(1);
    }
}
