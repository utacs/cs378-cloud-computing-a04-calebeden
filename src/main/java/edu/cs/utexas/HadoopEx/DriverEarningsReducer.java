package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DriverEarningsReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

    public void reduce(Text taxiId, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int total_entries = 0;
        int total_errors = 0;

        for (IntWritable value : values) {
            if (value.get() == 0) {
                total_entries++;
            } else {
                total_entries++;
                total_errors++;
            }
        }

        context.write(taxiId, new FloatWritable(((float) total_errors) / total_entries));

    }
}