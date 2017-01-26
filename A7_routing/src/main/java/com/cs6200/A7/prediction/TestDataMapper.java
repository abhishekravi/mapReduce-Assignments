package com.cs6200.A7.prediction;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Chintan Pathak, Chinmayee Vaidya
 *
 */

/**
 * The mapper class for the prediction phase to map the requests
 * This class does the following things:
 * 1) It reads the testing data from the testing dataset files
 * 2) Then it extracts the required attributes from the data that
 *    will be useful to the reducer for making prediction if the given
 *    connection would be delayed or not
 * 3) It constructs the data in a way that would partition it based 
 *    on the specific origins and destinations and the values used 
 *    would be of the type PredictionCompositeValue
 * 4) In this way each specific connection would go to a reducer with
 *    the possible request for it
 */
public class TestDataMapper extends Mapper<LongWritable, Text, Text, PredictionCompositeValue> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, PredictionCompositeValue>.Context context)
					throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");

		try {
			String key2;
			PredictionCompositeValue value2 = new PredictionCompositeValue();

			key2 = tokens[5] + "," + tokens[7];

			value2.uniqueCarrier = tokens[0];
			value2.dayOfWeek = Integer.parseInt(tokens[4]);
			value2.interm = tokens[6];
			value2.layoverHours = Integer.parseInt(tokens[8]);
			value2.totalElapsedTime = Integer.parseInt(tokens[9]);
			value2.totalElapsedHours = (value2.totalElapsedTime / 60);
			value2.totalDistanceGroup = Integer.parseInt(tokens[10]);
			value2.isMissed = (Integer.parseInt(tokens[13]) == 0) ? false : true;
			value2.firstFlightNo = (Integer.parseInt(tokens[11]));
			value2.secondFlightNo = (Integer.parseInt(tokens[12]));
			value2.type = 1;

			value2.year = -1;
			value2.month = Integer.parseInt(tokens[2]);
			value2.day = -1;

			context.write(new Text(key2), value2);
		} catch (NumberFormatException e) {
			System.err.println(
					"NumberFormatException occured in - TestDataMapper - with message - " + e.getLocalizedMessage());
		}
	}
}
