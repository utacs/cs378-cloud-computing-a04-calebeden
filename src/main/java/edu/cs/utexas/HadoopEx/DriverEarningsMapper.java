package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DriverEarningsMapper extends Mapper<Object, Text, Text, FloatArrayWritable> {

	// Create a counter and initialize with 1
	private Text driver = new Text();
	private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private FloatWritable tripTotal = new FloatWritable();
	private FloatWritable tripDuration = new FloatWritable();
	private FloatWritable[] valuesArray = new FloatWritable[2];
	private FloatArrayWritable values = new FloatArrayWritable();

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

		String taxiId = sections[0];
		String driverId = sections[1];

		String pickupLongString = sections[6];
		float pickupLong = Float.parseFloat(pickupLongString);
		String pickupLatString = sections[7];
		float pickupLat = Float.parseFloat(pickupLatString);
		String dropoffLongString = sections[8];
		float dropoffLong = Float.parseFloat(dropoffLongString);
		String dropoffLatString = sections[9];
		float dropoffLat = Float.parseFloat(dropoffLatString);

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

			if (pickupLong == 0.0 || pickupLat == 0.0 || dropoffLong == 0.0 || dropoffLat == 0.0) {
				return;
			}

			driver.set(new Text(driverId));
			tripTotal.set(total);
			tripDuration.set(duration);
			valuesArray[0] = tripTotal;
			valuesArray[1] = tripDuration;
			values.set(valuesArray);
			context.write(driver, values);

		} catch (ParseException e) {
			// throw new IllegalArgumentException("Error parsing date");
			return;
		}
	}
}