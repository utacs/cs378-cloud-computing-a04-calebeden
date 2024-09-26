package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DriverEarningsReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

    public void reduce(Text taxiId, Iterable<ArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        int total_money = 0;
        int total_time = 0;

        for (ArrayWritable value : values) {
            Writable[] writables = value.get();
            FloatWritable moneyWritable = (FloatWritable) writables[0];  
            FloatWritable timeWritable = (FloatWritable) writables[1];
            total_money += moneyWritable.get();
            total_time += timeWritable.get();
        }

        context.write(taxiId, new FloatWritable(( total_money * 60) / total_time));

    }
}