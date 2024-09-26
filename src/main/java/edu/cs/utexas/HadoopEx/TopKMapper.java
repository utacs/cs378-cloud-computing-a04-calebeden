package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {

	private Logger logger = Logger.getLogger(TopKMapper.class);


	private PriorityQueue<TaxiAndErrorRate> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>(10);

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {


		float errorRate = Float.parseFloat(value.toString());

		pq.add(new TaxiAndErrorRate(new Text(key), new FloatWritable(errorRate)) );

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			TaxiAndErrorRate taxiAndErrorRate = pq.poll();
			context.write(taxiAndErrorRate.getTaxiId(), taxiAndErrorRate.getErrorRate());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}