package edu.project1.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// VIVA NOTES - MapReduce Reducer
// * After the shuffle/sort, all 1s for one label arrive together; the reducer sums them. Because summing is associative it is also reused as the COMBINER
//   (a map-side mini-reduce that cuts shuffle traffic).
/**
 * JobStatusReducer — Phase 5 (MapReduce Baseline)
 *
 * INPUT:  KEY   = status label (SUCCESS / FAILED / KILLED / UNKNOWN_n)
 *         VALUE = Iterable of IntWritable(1) from all Mappers
 *
 * REDUCE: Sum all values for each key.
 *         Emit: KEY = status label, VALUE = total count
 *
 * OUTPUT: Written to /project/mapreduce_output/job_status_counts/part-r-00000
 *         Format: SUCCESS\t<count>
 *                 FAILED\t<count>
 *                 KILLED\t<count>
 */
public class JobStatusReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        result.set(sum);
        context.write(key, result);

        // Record per-key totals in counters for easy inspection in Hadoop UI
        context.getCounter("StatusCounts", key.toString()).increment(sum);
    }
}
