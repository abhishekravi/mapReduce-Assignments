package com.cs6200.A7.connections;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.cs6200.A7.A7;
import com.cs6200.A7.data.Constants;
import com.cs6200.A7.data.Flight;
import com.cs6200.A7.data.RecordData;
import com.cs6200.A7.exception.BadDataException;
import com.cs6200.tools.Utils;

/**
 * The driver class for the connections job of the program
 * 
 * @author Abhishek Ravi Chandran, Mania Abdi
 */
public class FindConnectionsJob extends Configured implements Tool {

	/**
	 * method that runs the mapreduce job
	 */
	public int run(String[] args) throws Exception {
		if (args.length != Constants.MRARGSIZE) {
			System.err.println("usage: <hinput dir> <tinput dir> <output dir> <bucket>");
			return -1;
		}
		String hinput = args[Constants.ARG2].split("=")[1];
		String tinput = args[Constants.ARG3].split("=")[1];
		String output = args[Constants.ARG4].split("=")[1];
		String bucket = args[Constants.ARG5].split("=")[1];

		Configuration config = getConf();
		config.set("bucket", bucket);
		config.set("mapreduce.output.textoutputformat.separator", ",");

		Job job = Job.getInstance(config, "Find connections");
		job.setSortComparatorClass(CkeyComparator.class);
		job.setGroupingComparatorClass(CWritableGroupComp.class);
		job.setPartitionerClass(CPartitioner.class);
		job.setNumReduceTasks(30);
		job.setJarByClass(A7.class);
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(CWritable.class);
		job.setReducerClass(ConnectionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(hinput), TextInputFormat.class, HConnectionMapper.class);
		MultipleInputs.addInputPath(job, new Path(tinput), TextInputFormat.class, TConnectionMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Mapper for job
	 * 
	 * this class reads all the records and only selects good records and groups
	 * them by carrier and year for the history files.
	 * 
	 * @author Abhishek Ravi Chandran
	 * 
	 */
	static class HConnectionMapper extends Mapper<LongWritable, Text, CustomKey, CWritable> {

		RecordData record = new RecordData();

		/**
		 * mapper to read historical data. key : carrier,year,record type
		 *
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				record.fill(Utils.parseCSV(value.toString()), 0);
				if (!record.isSane())
					return;
				context.write(new CustomKey(record, 0, 0), new CWritable(record, 0));
				context.write(new CustomKey(record, 1, 0), new CWritable(record, 1));
			} catch (BadDataException e) {
				return;
			}
		}
	}

	/**
	 * Mapper for job
	 * 
	 * this class reads all the records and only selects good records and groups
	 * them by carrier and year for the test files.
	 * 
	 * @author Abhishek Ravi Chandran
	 * 
	 */
	static class TConnectionMapper extends Mapper<LongWritable, Text, CustomKey, CWritable> {

		Flight record = new Flight();

		/**
		 * mapper to read test data. key : carrier,year,record type
		 *
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				record.fill(Utils.parseCSV(value.toString()), 0);
				context.write(new CustomKey(record, 0, 1), new CWritable(record, 0));
				context.write(new CustomKey(record, 1, 1), new CWritable(record, 1));
			} catch (BadDataException e) {
				return;
			}
		}
	}

	/**
	 * Reducer class that writes connections. using secondary sort we get
	 * incoming flights first and then outgoing flights
	 * 
	 * @author Abhishek Ravichandran
	 * @author Chinmayee Vaidya
	 */
	static class ConnectionReducer extends Reducer<CustomKey, CWritable, Text, Text> {

		FileSystem fs;

		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				fs = FileSystem.get(new URI(context.getConfiguration().get("bucket")), context.getConfiguration());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		};

		/**
		 * @param key
		 *            - single carrier and year
		 * @param values
		 *            - sorted data according to type and time
		 */
		public void reduce(CustomKey key, Iterable<CWritable> values, Context context)
				throws IOException, InterruptedException {
			// map to hold incoming flights
			Path file;
			String s[] = Utils.parseCSV(key.cyear);
			String type = s[2];
			// 0 for historical data and 1 for test data
			if (type.equalsIgnoreCase("0"))
				file = new Path("hmodel/" + s[0] + "," + s[1]);
			else
				file = new Path("tmodel/" + s[0] + "," + s[1]);

			if (fs.exists(file))
				fs.delete(file, true);
			OutputStream os = fs.create(file);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			// using a treeSet helps sort the data according to scheduled time
			Map<String, TreeSet<CWritable>> IMap = new HashMap<String, TreeSet<CWritable>>();
			TreeSet<CWritable> flights;
			for (CWritable ctw : values) {
				// collect all incoming flights into a map
				if (ctw.type == 0) {
					flights = IMap.get(ctw.city);
					if (flights == null)
						IMap.put(ctw.city, flights = new TreeSet<CWritable>(new TimeComp()));
					flights.add(new CWritable(ctw));
				} else {
					// start processing outgoing flights
					flights = IMap.get(ctw.city);
					if (flights == null)
						continue;
					for (CWritable inf : flights) {
						// since the data is sorted, we can break where the time
						// difference
						// goes beyond the max limit for a connection
						if (Utils.timeDiff(ctw.stime, inf.stime) > Constants.MAXCONNTIME)
							break;
						checkConnection(inf, ctw, s, bw, type);
					}
				}
			}
			bw.close();
		}

		/**
		 * Custom comparator to sort according to time in descending order.
		 * 
		 * @author Abhishek Ravi Chandran
		 *
		 */
		class TimeComp implements Comparator<CWritable> {

			@Override
			public int compare(CWritable e1, CWritable e2) {
				return (int) (e2.stime - e1.stime);
			}
		}

		/**
		 * check connection.
		 * 
		 * @param inf
		 *            incoming flight arriving at the destination
		 * @param outf
		 *            outgoing flight leaving that airport
		 * @param s
		 *            keys
		 * @param bw
		 *            file writer
		 * @param type
		 *            type of record
		 * @throws InterruptedException
		 * @throws IOException
		 */
		public void checkConnection(CWritable inf, CWritable outf, String[] s, BufferedWriter bw, String type)
				throws IOException, InterruptedException {
			int missed;
			// check connection window
			if (Utils.timeDiff(outf.stime, inf.stime) >= Constants.MINCONNTIME
					&& Utils.timeDiff(outf.stime, inf.stime) <= Constants.MAXCONNTIME) {
				missed = 0;
				if (type.equals("0")) {
					// check for bad connection
					if (inf.cancelled == 1 || (Utils.timeDiff(outf.atime, inf.atime) < Constants.MINCONNTIME))
						missed = 1;
				}
				// carrier, year, month, day, dayofweek, origin, intdest,
				// findest, layover, totalelapsedtime,
				// distgrp, inflightnum, outflightnum, missed
				String record = s[0] + "," + s[1] + "," + inf.month + "," + inf.day + "," + inf.dayofweek + ","
						+ inf.origin + "," + inf.dest + "," + outf.dest + "," + (outf.cdeptime - inf.carrtime) / 60
						+ "," + (inf.elapsed + outf.elapsed) + "," + +(inf.dist + outf.dist) + "," + inf.fnum + ","
						+ outf.fnum + "," + missed + "\n";
				bw.write(record);
			}
		}
	}
}
