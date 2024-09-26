package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HourGPSErrorMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private IntWritable time = new IntWritable();
	private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private final Calendar calendar = Calendar.getInstance();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] sections = value.toString().trim().split(",");

		if (sections.length != 17) {
			// throw new IllegalArgumentException("Error processing line (count)");
			return;
		}

		String fareString = sections[11];
		float fare = Float.parseFloat(fareString);
		String surchargeString = sections[12];
		float surcharge = Float.parseFloat(surchargeString);
		String taxString = sections[13];
		float tax = Float.parseFloat(taxString);
		String tipString = sections[14];
		float tip = Float.parseFloat(tipString);
		String tollsString = sections[15];
		float tolls = Float.parseFloat(tollsString);
		String totalString = sections[16];
		float total = Float.parseFloat(totalString);
		String durationString = sections[4];
		int duration = Integer.parseInt(durationString);
		if (Math.abs(fare + surcharge + tax + tip + tolls - total) > 0.001) {
			// throw new IllegalArgumentException("Error processing line (sum)");
			return;
		}
		if (total > 500) {
			// throw new IllegalArgumentException("Error processing line (total)");
			return;
		}
		if (duration == 0) {
			// throw new IllegalArgumentException("Error processing line (duration)");
			return;
		}

		String pickupLongString = sections[6];
		float pickupLong = pickupLongString.isBlank() ? 0.0f : Float.parseFloat(pickupLongString);
		String pickupLatString = sections[7];
		float pickupLat = pickupLatString.isBlank() ? 0.0f : Float.parseFloat(pickupLatString);
		String dropoffLongString = sections[8];
		float dropoffLong = dropoffLongString.isBlank() ? 0.0f : Float.parseFloat(dropoffLongString);
		String dropoffLatString = sections[9];
		float dropoffLat = dropoffLatString.isBlank() ? 0.0f : Float.parseFloat(dropoffLatString);

		try {
			String pickupDateString = sections[2];
			Date pickupDate = format.parse(pickupDateString);
			String dropoffDatetime = sections[3];
			Date dropoffDate = format.parse(dropoffDatetime);

			if (dropoffDate.before(pickupDate)) {
				// throw new IllegalArgumentException("Error: dropoff date is before pickup
				// date");
				return;
			}

			if (pickupLong == 0.0f || pickupLat == 0.0f) {
				calendar.setTime(pickupDate);
				time.set(calendar.get(Calendar.HOUR_OF_DAY) + 1);
				context.write(time, counter);
			}
			if (dropoffLong == 0.0f || dropoffLat == 0.0f) {
				calendar.setTime(dropoffDate);
				time.set(calendar.get(Calendar.HOUR_OF_DAY) + 1);
				context.write(time, counter);
			}
		} catch (ParseException e) {
			// throw new IllegalArgumentException("Error parsing date");
			return;
		}
	}
}