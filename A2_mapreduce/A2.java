import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * 
 * @author Abhishek Ravi chandran Class processing flight data using map reduce.
 *
 */
public class A2 extends Configured implements Tool {

	static Parser PARSER = new Parser();

	/**
	 * main method to process the data
	 * 
	 * @param args
	 *            file location
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new A2(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "");
		job.setJar("job.jar");
		//job.addArchiveToClassPath(new Path("/user/hduser/opencsv-3.6.jar"));
		job.setMapperClass(FlightDataMapper.class);
		job.setReducerClass(FlightDataReducer.class);
		//job.setReducerClass(FlightDataReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// getting all the files in the input directory
		FileInputFormat.setInputDirRecursive(job, true);
		// adding input files will be recursively processed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// setting up the output path
		FileOutputFormat.setOutputPath(job, new Path("output"));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 
	 * @author Abhishek Ravi Chandran this class reads all the records and only
	 *         selects good records and groups them by carrier and month.
	 *
	 */
	static class FlightDataMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			RecordData recordData = new RecordData();
			DataValidator dataValidator = new DataValidator();

			try {
				PARSER.getRecordData(value.toString(), recordData);
			} catch (BadDataException e) {
				return;
			} catch (NumberFormatException ne) {
				return;
			}
			if (dataValidator.isGoodRecord(recordData)) {
				// have key as carrier,month
				// passing along values average ticket price, year
				context.write(new Text(recordData.getCarrier() + ","
						+ recordData.getMonth()+","+recordData.getYear()+","),
						new Text(String.valueOf(recordData.getAvgTicketPrice())));
			}
		}

	}

	/**
	 * 
	 * @author Abhishek Ravichandran reducer class that calculates the mean
	 *         price for all carriers.
	 *
	 */
	static class FlightDataReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int size = 0;
			String data[];
			double total = 0;
			// getting number of flights per carrier
			for (Text v : values) {
				size++;
				data = PARSER.parseCSV(v.toString());
				total += Double.parseDouble(data[0]);
			}
			double mean = total / size;
			//op: carrier,month size,mean
			context.write(new Text(key), new Text(size + "," + mean));
		}
	}

}
