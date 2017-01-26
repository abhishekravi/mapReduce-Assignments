package com.cs6200.tools;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cs6200.A7.data.App;
import com.cs6200.A7.modeling.ModelingCompositeValue;
import com.cs6200.A7.prediction.PredictionCompositeValue;
import com.google.common.collect.Lists;
import com.opencsv.CSVParser;

import quickml.data.AttributesMap;
import quickml.data.instances.ClassifierInstance;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForest;

/**
 * 
 * @author Chintan Pathak, Abhishek Ravi Chandran
 * @author Mania Abdi, Chinmayee Vaidya
 *
 */

/*
 * This class provides various utility functions to use in doing the required
 * processing
 */
public class Utils {

	final static CSVParser PARSER = new CSVParser();

	/**
	 * method to parse csv data.
	 * 
	 * @param value
	 *            csv data
	 * @return string array
	 */
	public static String[] parseCSV(String value) {
		String parsed[] = null;
		try {
			parsed = PARSER.parseLine(value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return parsed;
	}

	/**
	 * get time difference in minutes (l1-l2)
	 * 
	 * @param l1
	 *            time in milliseconds
	 * @param l2
	 *            time in milliseconds
	 * @return Difference in minutes(l1-l2)
	 */
	public static long timeDiff(long l1, long l2) {
		return TimeUnit.MILLISECONDS.toMinutes(l1 - l2);
	}
	
	/**
	 * Normalizes the date string for error free parsing Ex: 015 -> 0015 15 ->
	 * 0015 0015 -> 0015
	 * 
	 * @param date
	 * @return String
	 */
	public static String normalizeDate(String date) {
		if (date.length() <= 2) {
			if (date.length() == 2) {
				date = "00" + date;
			} else {
				date = "000" + date;
			}
		} else {
			if (date.length() == 3) {
				date = "0" + date;
			}
		}
		return date;
	}

	/**
	 * Method to create a data set out of objects of class
	 * PredictionCompositeKey and PredictionCompositeValue that is to be used
	 * further to create a prediction model
	 * 
	 * @param key
	 * @param list
	 * @return List<ClassifierInstance>
	 */
	public static List<ClassifierInstance> loadDataSet(
			Iterable<ModelingCompositeValue> list) {
		List<ClassifierInstance> instances = Lists.newLinkedList();
		AttributesMap attributes;
		Set<String> variety = new HashSet<String>();

		for (ModelingCompositeValue item : list) {
			attributes = AttributesMap.newHashMap();
			attributes.put("day_of_week", item.dayOfWeek);
			attributes.put("origin", item.origin);
			attributes.put("intermediate", item.interm);
			attributes.put("dest", item.dest);
			attributes.put("layover_hours", item.layoverHours);
			attributes.put("total_elapsed_hours", item.totalElapsedHours);
			attributes.put("total_distance_group", item.totalDistanceGroup);
			attributes.put("delayed", item.isMissed);

			variety.add(item.isMissed);
			instances.add(new ClassifierInstance(attributes, item.isMissed));
		}
		System.err.println(variety.size());
		if (variety.size() >= 2) {
			return instances;
		} else {
			return null;
		}
	}

	/**
	 * This method serializes the passed object to the folder serializedObjects/
	 * in the bucket set by the configuration parameter "bucket.name"
	 * 
	 * @param key
	 * @param object
	 * @param conf
	 */
	public static <T> void serializeObject(String key, T object, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(new URI(conf.get("bucket.name")), conf);
			DataOutputStream outS = fs.create(new Path("serializedObjects/" + key));
			 ObjectOutputStream out = new ObjectOutputStream(outS);

			 out.writeObject(object);
			 out.close();
			outS.close();
		} catch (URISyntaxException ex) {
			System.err.println("URI Syntax Exception with message : " + ex.getLocalizedMessage());
		} catch (IOException ex) {
			System.err.println("IOException with message : " + ex.getLocalizedMessage());
		}
	}

	/**
	 * This method checks if the required file exists inside the folder
	 * serializedObjects/ of the bucket set by the configuration parameter
	 * "bucket.name"
	 * 
	 * @param value
	 * @param object
	 * @param conf
	 */
	public static boolean forestExists(PredictionCompositeValue value, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(new URI(conf.get("bucket.name")), conf);
			return fs.exists(new Path("serializedObjects/" + value.uniqueCarrier + "_" + value.month));
		} catch (IOException e) {
		} catch (URISyntaxException e) {
		}
		return false;
	}

	/**
	 * This method deserializes the object stored inside the folder
	 * serializedObjects/ of the bucket set by the configuration parameter
	 * "bucket.name" with the required file name
	 * 
	 * @param value
	 * @param object
	 * @param conf
	 */
	public static RandomDecisionForest deserializeObject(PredictionCompositeValue value, Configuration conf) {
		Object obj = null;
		try {
			FileSystem fs = FileSystem.get(new URI(conf.get("bucket.name")), conf);
			DataInputStream inS = fs.open(new Path("serializedObjects/" + value.uniqueCarrier + "_" + value.month));
			ObjectInputStream in = new ObjectInputStream(inS);

			obj = in.readObject();
			in.close();
			inS.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			c.printStackTrace();
		} catch (URISyntaxException ex) {
			System.err.println("URI Syntax Exception with message : " + ex.getLocalizedMessage());
		}
		return (RandomDecisionForest) obj;
	}

	/**
	 * This method tosses a coin and returns true/false randomly
	 * 
	 * @return boolean
	 */
	public static String tossACoin() {
		return (((int) (Math.random() * 1000)) % 2 == 0) ? "1" : "0";
	}

	public static void runApp(App app, Object... args) {
		app.runApp(args);
	}
}
