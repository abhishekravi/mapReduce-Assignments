import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * driver class for processing flight data using map reduce.
 * 
 * @author Abhishek Ravi chandran
 * @author Chinmayee Vaidya
 *
 */
public class A4 extends Configured implements Tool {

	/**
	 * main method to process the data
	 * 
	 * @param args
	 *            file location
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == Constants.MRARGSIZE) {
			System.exit(ToolRunner.run(new A4(), args));
		} else {
			System.out
					.println("usage: <input dir> <output dir> -time=N <hdfsRoot/bucket> secretKey secretID");
		}

	}

	/**
	 * method that runs the mapreduce job
	 */
	public int run(String[] args) throws Exception {
		String input = args[Constants.ARG1].split("=")[1];
		String output = args[Constants.ARG2].split("=")[1];
		String root = args[Constants.ARG4];
		String skey = args[Constants.ARG5];
		String sid = args[Constants.ARG6];
		Configuration config = getConf();
		config.setInt("TIME",
				Integer.parseInt(args[Constants.ARG3].split("=")[1]));
		Job job1 = Job.getInstance(config, "");
		job1.setJar("job.jar");
		// adding mapper 1
		job1.setMapperClass(RegressionMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(RegressionReducer.class);
		// setting custom writable class for composite key
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		// getting all the files in the input directory
		FileInputFormat.setInputDirRecursive(job1, true);
		// adding input files will be recursively processed
		FileInputFormat.addInputPath(job1, new Path(input));
		// setting up the output path
		FileOutputFormat.setOutputPath(job1, new Path(output));
		job1.waitForCompletion(true);
		AWSHelperClass a = new AWSHelperClass(sid,skey);
		String carrier = a.findCheapest(root,output);
		config.set("CARRIER", carrier);
		Job job2 = Job.getInstance(config, "");
		if (job1.isSuccessful()) {
			job2.setJar("job.jar");
			// adding mapper 1
			job2.setMapperClass(PlotMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(FloatWritable.class);
			job2.setReducerClass(PlotReducer.class);
			// setting custom writable class for composite key
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(FloatWritable.class);
			// getting all the files in the input directory
			FileInputFormat.setInputDirRecursive(job2, true);
			// adding input files will be recursively processed
			FileInputFormat.addInputPath(job2, new Path(input));
			// setting up the output path
			FileOutputFormat.setOutputPath(job2, new Path(output + "_final"));
		}
		if (job2.waitForCompletion(true)) {
			Process p;
			p = Runtime.getRuntime().exec("hadoop fs -get output_final");
			p.waitFor();
			p = Runtime.getRuntime().exec("make graph");
			p.waitFor();
			return 0;
		} else {
			return 1;
		}
	}

	/*************************************
	 * mapper and reducer for job1****************************
	 * 
	 * /** this class reads all the records and only selects good records and
	 * groups them by carrier and year.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class RegressionMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RecordData recordData = new RecordData();

			try {
				Parser.getRecordData(value.toString(), recordData);
			} catch (BadDataException e) {
				// skip all bad records
				return;
			}
			if (DataValidator.isGoodRecord(recordData)) {
				// have key as year, carrier
				// passing along values average ticket price
				context.write(
						new Text(recordData.getYear() + " "
								+ recordData.getCarrier()),
						new Text(recordData.getCrsElapsedTime() + ","
								+ recordData.getAvgTicketPrice()));
			}
		}

	}

	/**
	 * reducer class that calculates the mean/median price for all carriers per
	 * month.
	 * 
	 * @author Abhishek Ravichandran
	 *
	 */
	static class RegressionReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int n = context.getConfiguration().getInt("TIME", 0);
			// getting number of flights per carrier
			/*
			 * SimpleRegression regression = new SimpleRegression(); for (Text v
			 * : values) { String [] s = Util.parseCSV(v.toString()); float time
			 * = Float.parseFloat(s[0]); float price = Float.parseFloat(s[1]);;
			 * regression.addData(time,price); }
			 */
			double sumx = 0;
			double sumy = 0;
			double sumxy = 0;
			double sumxx = 0;
			long num = 0;
			for (Text v : values) {
				String[] s = Util.parseCSV(v.toString());
				float time = Float.parseFloat(s[0]);
				float price = Float.parseFloat(s[1]);
				;
				num++;
				sumx += time;
				sumy += price;
				sumxy += time * price;
				sumxx += time * time;
			}
			double slope = LinearRegression.calculateSlope(num, sumx, sumy,
					sumxy, sumxx);
			double intercept = LinearRegression.calculateIntercept(num, sumx,
					sumy, slope);
			// op: year,carrier predicted price for time n
			// context.write(key, new
			// Text(String.valueOf(regression.predict(n))));
			context.write(
					key,
					new Text(String.valueOf(LinearRegression
							.calculateProjectedScoreForTargetPeriod(slope,
									intercept, n))));
		}
	}

	/*************************************
	 * mapper and reducer for job2****************************
	 * 
	 * /** this class reads all the records and only selects good records and
	 * groups them by carrier and year.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class PlotMapper extends
			Mapper<LongWritable, Text, Text, FloatWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RecordData recordData = new RecordData();
			String car = context.getConfiguration().get("CARRIER");
			try {
				Parser.getRecordData(value.toString(), recordData);
			} catch (BadDataException e) {
				// skip all bad records
				return;
			}
			if (DataValidator.isGoodRecord(recordData)
					&& recordData.getCarrier().equals(car)) {
				// have key as year, month, week, carrier
				// passing along values average ticket price
				Calendar c = Calendar.getInstance();
				c.set(Calendar.YEAR, recordData.getYear());
				c.set(Calendar.MONTH, recordData.getMonth() - 1);
				c.set(Calendar.DATE, recordData.getDay());
				context.write(
						new Text(recordData.getYear() + " "
								+ c.get(Calendar.WEEK_OF_YEAR) + " "
								+ recordData.getCarrier()), new FloatWritable(
								recordData.getAvgTicketPrice()));
			}
		}

	}

	/**
	 * reducer class that calculates the mean/median price for all carriers per
	 * month.
	 * 
	 * @author Abhishek Ravichandran
	 *
	 */
	static class PlotReducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float median = 0;
			List<Float> prices = new ArrayList<Float>();
			for (FloatWritable v : values) {
				prices.add(v.get());
			}
			median = Util.fastMedian(prices);
			// op: year,month, week, carrier median
			context.write(key, new FloatWritable(median));
		}
	}

}
