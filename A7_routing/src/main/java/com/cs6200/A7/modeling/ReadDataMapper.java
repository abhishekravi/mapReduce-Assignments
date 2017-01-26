package com.cs6200.A7.modeling;

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
 * The mapper class for the modeling phase 
 * This class does the following things:
 * 1) It reads the training data from the training dataset files  
 * 2) Then it extracts the required attributes from the data 
 *    that will be useful to the reducer for generating a decision 
 *    making prediction model 
 * 4) It constructs the data in a way that would partition it 
 *    on the key being of type Text and
 *    the values being of the type PredictionCompositeValue
 */
public class ReadDataMapper extends Mapper<LongWritable, Text, Text, ModelingCompositeValue> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, ModelingCompositeValue>.Context context)
					throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");

		try {
			String key2;
			ModelingCompositeValue value2 = new ModelingCompositeValue();
			key2 = tokens[0] + "_" + Integer.parseInt(tokens[2]);

			value2.dayOfWeek = Integer.parseInt(tokens[4]);
			value2.origin = tokens[5];
			value2.interm = tokens[6];
			value2.dest = tokens[7];
			value2.layoverHours = (Integer.parseInt(tokens[8]));
			value2.totalElapsedHours = (Integer.parseInt(tokens[9]) / 60);
			value2.totalDistanceGroup = Integer.parseInt(tokens[10]);
			value2.isMissed = tokens[13];

			context.write(new Text(key2), value2);
		} catch (NumberFormatException e) {
			System.err.println(
					"NumberFormatException occured in - ReadDataMapper - with message - " + e.getLocalizedMessage());
		}
	}
}
