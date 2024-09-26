package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxiGPSErrorReducer extends  Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

   public void reduce(IntWritable time, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int sum = 0;
       
       for (IntWritable value : values) {
           sum += value.get();
       }
       
       context.write(time, new IntWritable(sum));
   }
}