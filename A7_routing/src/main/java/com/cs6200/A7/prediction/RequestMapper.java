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
 * 1) It reads the requests from the requests file
 * 2) Then it extracts the required attributes from the data that
 *    will be useful to the reducer for making prediction if the given
 *    connection would be delayed or not
 * 3) It constructs the data in a way that would partition it based 
 *    on the specific origins and destinations and the values used 
 *    would be of the type PredictionCompositeValue
 * 4) In this way each specific request would go to a reducer with
 *    the possible bunch of connections for it
 */
public class RequestMapper extends Mapper<LongWritable, Text, Text, PredictionCompositeValue> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, PredictionCompositeValue>.Context context)
					throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");

		try {
			String key2;
			PredictionCompositeValue value2 = new PredictionCompositeValue();
			
			key2 = tokens[3] + "," + tokens[4];
			
			value2.uniqueCarrier = "";
			value2.dayOfWeek = -1;
			value2.interm = "";
			value2.layoverHours = -1;
			value2.totalElapsedTime = -1;
			value2.totalElapsedHours = -1;
			value2.totalDistanceGroup = -1;
			value2.isMissed = true;
			value2.firstFlightNo = -1;
			value2.secondFlightNo = -1;
			value2.type = 0;
			
			value2.year = Integer.parseInt(tokens[0]);
			value2.month = Integer.parseInt(tokens[1]);
			value2.day = Integer.parseInt(tokens[2]);
			
			context.write(new Text(key2), value2);
		} catch (NumberFormatException e) {
			System.err.println("NumberFormatException occured in - RequestMapper - with message - " + e.getLocalizedMessage());
		} 
	}
}
