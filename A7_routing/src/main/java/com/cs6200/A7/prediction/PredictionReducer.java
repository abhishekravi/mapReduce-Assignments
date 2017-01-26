package com.cs6200.A7.prediction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cs6200.tools.Utils;

import quickml.data.AttributesMap;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForest;

/**
 * 
 * @author Chintan Pathak, Chinmayee Vaidya
 *
 */

/**
 * The reducer class for the prediction phase 
 * This class does the following things: 
 * 1) It gets the details about each test connections and the requests
 *    falling under the same source and destination 
 * 2) It creates a attributes map, using which the corresponding model 
 *    would be able to make a prediction for the connection to be missed 
 *    or be successful
 * 3) It uses the carrier name and month of the flight to find the 
 *    correct serialized object using which the prediction is to be made 
 * 4) If the corresponding object is found then the prediction is made using 
 *    that else the method of coin toss is used for each
 *    entries corresponding to this flight carrier's list of flights
 * 5) After predicting all the possible connections for a given
 *    request it picks the best connection and writes it to the output   
 * 6) It also caches the random decision forest objects in case 
 *    they were already serialized by this reducer to minimize 
 *    the networking overhead of serializing the same object multiple times 
 */
public class PredictionReducer extends Reducer<Text, PredictionCompositeValue, Text, NullWritable> {

	private static Map<String, RandomDecisionForest> cache;

	/**
	 * Setup method to initialize the cache
	 */
	@Override
	protected void setup(Reducer<Text, PredictionCompositeValue, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		cache = new HashMap<String, RandomDecisionForest>();
	}

	/**
	 * The reduce method to pick and predict the best possible connection
	 * for a specific request
	 */
	@Override
	protected void reduce(Text key, Iterable<PredictionCompositeValue> values,
			Reducer<Text, PredictionCompositeValue, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {

		Map<PredictionCompositeValue, String> connections = new HashMap<PredictionCompositeValue, String>();
		PredictionCompositeValue request = null;
		String[] keyVal = key.toString().split(",");

		for (PredictionCompositeValue value : values) {
			if (value.type == 0) {
				request = new PredictionCompositeValue(value);
				continue;
			}

			AttributesMap attributes = getAttributesMap(keyVal, value);

			if (isCached(value)) {
				RandomDecisionForest randomForest = cache.get(value.uniqueCarrier + "_" + value.month);
				connections.put(new PredictionCompositeValue(value),
						randomForest.getClassificationByMaxProb(attributes).toString());
			} else {
				if (Utils.forestExists(value, context.getConfiguration())) {
					RandomDecisionForest randomForest = Utils.deserializeObject(value, context.getConfiguration());
					cache.put(value.uniqueCarrier + "_" + value.month, randomForest);
					connections.put(new PredictionCompositeValue(value),
							randomForest.getClassificationByMaxProb(attributes).toString());
				} else {
					connections.put(new PredictionCompositeValue(value), Utils.tossACoin());
				}
			}
		}

		writeOutput(request, connections, context, key.toString());
	}

	/**
	 *  Picks the best connection out of the connections map and writes
	 *  it to the output using the required format
	 * @param request
	 * @param connections
	 * @param context
	 * @param key
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void writeOutput(PredictionCompositeValue request, Map<PredictionCompositeValue, String> connections,
			Reducer<Text, PredictionCompositeValue, Text, NullWritable>.Context context, String key)
					throws IOException, InterruptedException {
		PredictionCompositeValue chosen = null;
		if (request != null) {
			chosen = selectFirst(connections);
			chosen = findTheBest(chosen, connections);

			if (chosen != null) {
				context.write(
						new Text(request.year + "," + request.month + "," + request.day + "," + key + ","
								+ chosen.firstFlightNo + "," + chosen.secondFlightNo + "," + chosen.totalElapsedTime),
						NullWritable.get());
			}
		}
	}

	/**
	 * Generates an attribute map using the key and value 
	 * which will be further used to classify the corresponding
	 * as being missed or being successful using the required
	 * prediction model
	 * 
	 * @param keyVal
	 * @param value
	 * @return AttributesMap
	 */
	private static AttributesMap getAttributesMap(String[] keyVal, PredictionCompositeValue value) {
		AttributesMap attributes = AttributesMap.newHashMap();
		attributes.put("day_of_week", value.dayOfWeek);
		attributes.put("origin", keyVal[0]);
		attributes.put("intermediate", value.interm);
		attributes.put("dest", keyVal[1]);
		attributes.put("layover_hours", value.layoverHours);
		attributes.put("total_elapsed_hours", value.totalElapsedHours);
		attributes.put("total_distance_group", value.totalDistanceGroup);
		attributes.put("delayed", "?");
		return attributes;
	}

	/**
	 * Checks if the required classifier is already cached or not
	 * @param value 
	 * @return boolean
	 */
	private static boolean isCached(PredictionCompositeValue value) {
		String fileName = value.uniqueCarrier + "_" + value.month;
		return cache.containsKey(fileName);
	}

	/**
	 * Selects the first connection from the map
	 * @param connections
	 * @return PredictionCompositeValue
	 */
	private static PredictionCompositeValue selectFirst(Map<PredictionCompositeValue, String> connections) {
		PredictionCompositeValue chosen = null;
		for (PredictionCompositeValue connection : connections.keySet()) {
			if (connections.get(connection).equalsIgnoreCase("0")) {
				chosen = connection;
				break;
			}
			chosen = connection;
		}
		return chosen;
	}

	/**
	 * Selects the best possible connection from the map based on
	 * -- It should be "not missed"
	 * -- It should have the shortest "total elapsed time" 
	 * @param connections
	 * @return PredictionCompositeValue
	 */
	private static PredictionCompositeValue findTheBest(PredictionCompositeValue chosen,
			Map<PredictionCompositeValue, String> connections) {
		for (PredictionCompositeValue connection : connections.keySet()) {
			if (connections.get(connection).equalsIgnoreCase("0")
					&& connection.totalElapsedTime < chosen.totalElapsedTime) {
				chosen = connection;
			}
		}
		return chosen;
	}
}
