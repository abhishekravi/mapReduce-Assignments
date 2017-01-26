import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
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
 * driver class for processing flight data using map reduce.
 * 
 * @author Abhishek Ravi chandran
 * @author Chinmayee Vaidya
 * 
 */
public class A5 extends Configured implements Tool {

	/**
	 * main method to process the data
	 * 
	 * @param args
	 *            file location
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == Constants.MRARGSIZE) {
			System.exit(ToolRunner.run(new A5(), args));
		} else {
			System.out.println("usage: <input dir> <output dir>");
		}

	}

	/**
	 * method that runs the mapreduce job
	 */
	public int run(String[] args) throws Exception {
		String input = args[Constants.ARG1].split("=")[1];
		String output = args[Constants.ARG2].split("=")[1];
		Configuration config = getConf();
		config.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(config, "");
		job.setSortComparatorClass(CkeyComparator.class);
		job.setGroupingComparatorClass(CWritableGroupComp.class);
		job.setPartitionerClass(CPartitioner.class);
		
		job.setJarByClass(A5.class);
		job.setMapperClass(ConnectionMapper.class);
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(CWritable.class);
		
		job.setReducerClass(ConnectionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * mapper for job
	 * 
	 * /** this class reads all the records and only selects good records and
	 * groups them by carrier and year. Send each record twice with flag 0 for
	 * incoming flights and 1 for outgoing flights
	 * 
	 * @author Abhishek Ravi Chandran
	 * 
	 */
	static class ConnectionMapper extends
			Mapper<LongWritable, Text, CustomKey, CWritable> {

		RecordData record = new RecordData();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				record.fill(Util.parseCSV(value.toString()), 0);
				if (!record.isSane())return;
				context.write(new CustomKey(record, 0),	new CWritable(record, 0));
				context.write(new CustomKey(record, 1),	new CWritable(record, 1));
			} catch (BadDataException e) {return;}
		}
	}

	/**
	 * reducer class that just checks if the connections is good and gives
	 * count.
	 * 
	 * @author Abhishek Ravichandran
	 * @author Chinmayee Vaidya
	 */
	static class ConnectionReducer extends
			Reducer<CustomKey, CWritable, Text, Text> {

		long count = 0;
		long bad = 0;
		
		/**
		 * @param key - single carrier and year
		 * @param values - sorted data according to type and time
		 */
		public void reduce(CustomKey key, Iterable<CWritable> values,
				Context context) throws IOException, InterruptedException {
			//map to hold incoming flights
			Map<String, TreeSet<CWritable>> IMap = new HashMap<String, TreeSet<CWritable>>();
			TreeSet<CWritable> flights;
			for (CWritable ctw : values) {
				if (ctw.type == 0) {
					flights = IMap.get(ctw.city);
					if (flights == null)IMap.put(ctw.city, flights = new TreeSet<CWritable>(Collections.reverseOrder()));
					flights.add(new CWritable(ctw));
				} else {
					flights = IMap.get(ctw.city);
					if(flights == null) continue;
					for (CWritable inf : flights) {
						if (Util.timeDiff(ctw.stime, inf.stime) > Constants.MAXCONNTIME)break;
						checkConnection(inf, ctw);
					}
				}
			}
			context.write(new Text(key.cyear), new Text(count + "," + bad));
		}
		
		/**
		 * check connection.
		 * @param inf
		 * incoming flight arriving at the destination
		 * @param outf
		 * outgoing flight leaving that airport 
		 */
		public void checkConnection(CWritable inf, CWritable outf) {
			//if (a.cancelled && b.cancelled)	return;
			if (Util.timeDiff(outf.stime, inf.stime) >= Constants.MINCONNTIME
					&& Util.timeDiff(outf.stime, inf.stime) <= Constants.MAXCONNTIME) {
				count++;
				if (inf.cancelled ==1 || (Util.timeDiff(outf.atime, inf.atime) < Constants.MINCONNTIME)) bad++;
			}
		}
	}

}
