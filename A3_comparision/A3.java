import java.io.IOException;
import java.util.ArrayList;
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
public class A3 extends Configured implements Tool {

	static String MODE;

	/**
	 * main method to process the data
	 * 
	 * @param args
	 *            file location
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == Constants.MRARGSIZE) {
			System.exit(ToolRunner.run(new A3(), args));
		} else {
			System.out.println("usage: <mode> <input dir> <output dir>");
		}

	}

	/**
	 * method that runs the mapreduce job
	 */
	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		config.setStrings("MODE", args[Constants.ARG3]);
		Job job = Job.getInstance(config, "");
		job.setJar("job.jar");
		job.setMapperClass(FlightDataMapper.class);
		job.setReducerClass(FlightDataReducer.class);
		//setting custom writable class for composite key
		job.setMapOutputKeyClass(CarrierMonthWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		// getting all the files in the input directory
		FileInputFormat.setInputDirRecursive(job, true);
		// adding input files will be recursively processed
		FileInputFormat.addInputPath(job, new Path(args[Constants.ARG1].split("=")[1]));
		// setting up the output path
		FileOutputFormat.setOutputPath(job, new Path(args[Constants.ARG2].split("=")[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * this class reads all the records and only selects good records and groups
	 * them by carrier and month.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class FlightDataMapper extends
			Mapper<LongWritable, Text, CarrierMonthWritable, FloatWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RecordData recordData = new RecordData();

			try {
				Parser.getRecordData(value.toString(), recordData);
			} catch (BadDataException e) {
				//skip all bad records
				return;
			} 
			if (DataValidator.isGoodRecord(recordData)) {
				// have key as carrier,month
				// passing along values average ticket price
				context.write(new CarrierMonthWritable(recordData.getMonth(),
						recordData.getCarrier()),
						new FloatWritable(recordData.getAvgTicketPrice()));
			}
		}

	}

	/**
	 * reducer class that calculates the mean/median price for all carriers per month.
	 * 
	 * @author Abhishek Ravichandran
	 *
	 */
	static class FlightDataReducer extends
			Reducer<CarrierMonthWritable, FloatWritable, Text, FloatWritable> {

		public void reduce(CarrierMonthWritable key,
				Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			String mode = context.getConfiguration().get("MODE");
			long size = Constants.ZERO;
			double total = Constants.ZERO;
			List<Float> prices = new ArrayList<Float>();
			// getting number of flights per carrier
			for (FloatWritable v : values) {
				size++;
				total += v.get();
				prices.add(v.get());
			}
			//statistical variable to hold mean or median
			float statVal = Constants.ZERO;
			// op: month ,carrier, mean(or)median
			switch (mode) {
			case "-mn":
				statVal = (float) (total / size);
				context.write(new Text(key.toString()), new FloatWritable(
						statVal));
				break;
			case "-md":
				statVal = Util.calculateMedian(prices);
				context.write(new Text(key.toString()), new FloatWritable(
						statVal));
				break;
			case "-fm":
				statVal = Util.fastMedian(prices);
				context.write(new Text(key.toString()), new FloatWritable(
						statVal));
				break;

			}
		}
	}

}
