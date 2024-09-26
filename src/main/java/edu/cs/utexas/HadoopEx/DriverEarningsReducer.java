package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DriverEarningsReducer extends Reducer<Text, FloatArrayWritable, Text, FloatWritable> {

    public void reduce(Text taxiId, Iterable<FloatArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        float total_money = 0;
        float total_time = 0;

        for (FloatArrayWritable value : values) {
            Writable[] writables = value.get();
            FloatWritable moneyWritable = (FloatWritable) writables[0];  
            FloatWritable timeWritable = (FloatWritable) writables[1];
            total_money += moneyWritable.get();
            total_time += timeWritable.get();
        }

        context.write(taxiId, new FloatWritable(( total_money * 60) / total_time));

    }
}