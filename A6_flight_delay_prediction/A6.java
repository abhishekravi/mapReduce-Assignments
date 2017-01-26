import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.Loader;

/**
 * driver class for map reduce program to predict flight delay.
 * 
 * @author Abhishek Ravi chandran
 * @author Chinmayee Vaidya
 *
 */
public class A6 extends Configured implements Tool {

	/**
	 * main driver method.
	 * 
	 * @param args
	 *            file locations,mode
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == Constants.MRARGSIZE) {
			System.exit(ToolRunner.run(new A6(), args));
		} else {
			System.out.println("usage: <input=history input dir> <output=output> <tinput=test input>"
			+ " <awsid=awsid> <awskey=awskey> <bucket=bucket>  <mode=mode>");
		}

	}

	/**
	 * method that runs the mapreduce job
	 */
	public int run(String[] args) throws Exception {
		String hinput = args[Constants.ARG1].split("=")[1];
		String output = args[Constants.ARG2].split("=")[1];
		String tinput = args[Constants.ARG3].split("=")[1];
		String awsid = args[Constants.ARG4].split("=")[1];
		String awskey = args[Constants.ARG5].split("=")[1];
		String bucket = args[Constants.ARG6].split("=")[1];
		String mode = args[Constants.ARG7].split("=")[1];
		Configuration config = getConf();
		config.set("mode", mode);
		config.set("awsid", awsid);
		config.set("awskey", awskey);
		config.set("bucket", bucket);
		// changing separator to ,
		config.set("mapreduce.output.textoutputformat.separator", ",");
		Job job1 = Job.getInstance(config, "");
		job1.setJar("job.jar");
		job1.setMapperClass(ModelMapper.class);
		job1.setReducerClass(ModelReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(36);
		FileInputFormat.addInputPath(job1, new Path(hinput));
		FileOutputFormat.setOutputPath(job1, new Path(output));
		job1.waitForCompletion(true);
		Job job2 = Job.getInstance(config, "");
		if (job1.isSuccessful()) {
			job2.setJar("job.jar");
			job2.setMapperClass(PredMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setReducerClass(PredReducer.class);
			if (!mode.equals("pseudo"))
				job2.setNumReduceTasks(40);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(tinput));
			FileOutputFormat.setOutputPath(job2, new Path(output + "_final"));
		}
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Job1 Mapper Used to build the data model for predicting.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class ModelMapper extends Mapper<LongWritable, Text, Text, Text> {

		RecordData record = new RecordData();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				record.fill(Util.parseCSV(value.toString()), 0);
				if (!record.isSane() || record.cancelled) return;
				Text newKey = new Text(record.carrier + "," + record.quarter);
				Text newVal = getVal();
				context.write(newKey, newVal);
			} catch (BadDataException e) {return;}
		}

		/**
		 * get the data that has to passed in values.
		 * 
		 * @return value to be passed
		 */
		private Text getVal() {
			int et = (record.crsElapsedTime / 10) > 53 ? 53	: (record.crsElapsedTime / 10);
			int dist = (record.distance / 10) > 53 ? 53	: (record.distance / 10);
			int holiday = Util.nearHoliday(record.month, record.day,record.year);
			String origin = Constants.CITIES.contains(record.origin) ? record.origin: "??";
			String dest = Constants.CITIES.contains(record.dest) ? record.dest: "??";
			String late = record.arrDelay > 0 ? "TRUE" : "FALSE";
			return new Text(record.month + "," + record.week + "," 
			+ (record.cat / 100) + "," + (record.cdt / 100) + ","
			+ et + "," + dist + "," + origin + "," + dest + ","
			+ holiday + "," + late);
		}

	}

	/**
	 * reducer class to build model.
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class ModelReducer extends Reducer<Text, Text, Text, Text> {

		String awsid = "";
		String awskey = "";
		String bucket = "";
		String mode = "";
		AWSHelperClass a;

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			awsid = context.getConfiguration().get("awsid");
			awskey = context.getConfiguration().get("awskey");
			bucket = context.getConfiguration().get("bucket");
			mode = context.getConfiguration().get("mode");
			if (!mode.equals("pseudo"))
				a = new AWSHelperClass(awsid, awskey, bucket);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path file = new Path("histtemp/" + key.toString() + ".arff");
			if (fs.exists(file)) fs.delete(file, true);
			OutputStream os = fs.create(file);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,"UTF-8"));
			bw.write("@relation '" + key.toString() + "'");	bw.newLine();
			bw.write(Constants.ATTRIBUTES);
			bw.write("@data");	bw.newLine();
			for (Text t : values) {
				bw.write(key.toString() + "," + t.toString()); bw.newLine();
			}
			bw.close();
			fs.close();
			// if in cloud mode, upload file to s3
			if (mode.equals("pseudo"))	return;
			fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream f = fs.open(file);
			a.upload(key.toString() + ".arff", f, "temp");
			f.close();
		}
	}

	/**
	 * mapper for job2
	 * 
	 * this class predicts delays using weka
	 * 
	 * @author Abhishek Ravi Chandran
	 *
	 */
	static class PredMapper extends Mapper<LongWritable, Text, Text, Text> {

		Flight flight = new Flight();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				flight.fill(Util.parseCSV(value.toString()), 1);
				Text newKey = new Text(flight.carrier + "," + flight.quarter);
				context.write(newKey, getVal());
			} catch (BadDataException e) {return;}
		}

		/**
		 * get the data that has to passed in values.
		 * 
		 * @return value to be passed
		 */
		private Text getVal() {
			int et = (flight.crsElapsedTime / 10) > 53 ? 53	: (flight.crsElapsedTime / 10);
			int dist = (flight.distance / 10) > 53 ? 53	: (flight.distance / 10);
			int holiday = Util.nearHoliday(flight.month, flight.day,flight.year);
			String origin = Constants.CITIES.contains(flight.origin) ? flight.origin: "??";
			String dest = Constants.CITIES.contains(flight.dest) ? flight.dest: "??";
			String late = flight.arrDelay > 0 ? "TRUE" : "FALSE";
			return new Text(flight.month + "," + flight.week + "," 
			+ (flight.cat / 100) + "," + (flight.cdt / 100) + ","
			+ et + "," + dist + "," + origin + "," + dest + ","	+ holiday + 
			"," + late + "=" + flight.fnum + "_"+ flight.fldate + "_" + flight.cdt);
		}

	}

	/**
	 * reducer class that predicts delay.
	 * 
	 * @author Abhishek Ravichandran
	 *
	 */
	static class PredReducer extends Reducer<Text, Text, Text, Text> {

		String awsid = "";
		String awskey = "";
		String bucket = "";
		String mode = "";
		AWSHelperClass a;
		private static final Log LOGGER = LogFactory.getLog(PredReducer.class);
		List<String> okeysp = new ArrayList<String>();

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			awsid = context.getConfiguration().get("awsid");
			awskey = context.getConfiguration().get("awskey");
			bucket = context.getConfiguration().get("bucket");
			mode = context.getConfiguration().get("mode");
			if (!mode.equals("pseudo"))
				a = new AWSHelperClass(awsid, awskey, bucket);
		}

		/**
		 * reducer method.
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path file = new Path("testtemp/" + key.toString() + ".arff");
			if (fs.exists(file))fs.delete(file, true);
			OutputStream os = fs.create(file);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,"UTF-8"));
			bw.write("@relation '" + key.toString() + "'");	bw.newLine();
			bw.write(Constants.ATTRIBUTES);
			bw.write("@data");bw.newLine();
			String val[];
			for (Text t : values) {
				val = t.toString().split("=");
				this.okeysp.add(val[1]);
				bw.write(key.toString() + "," + val[0]);bw.newLine();
			}
			bw.close();
			fs.close();
			predictDelay(key.toString(), context);
		}

		/**
		 * method to predict delay.
		 * 
		 * @param filename
		 *            file to write
		 * @param context
		 *            for writing output
		 */
		private void predictDelay(String filename, Context context) {
			FileSystem fs;
			Path file;
			try {
				// loading training data
				ArffLoader trainl = new ArffLoader();
				if (mode.equals("pseudo")) {
					fs = FileSystem.get(context.getConfiguration());
					file = new Path("histtemp/" + filename + ".arff");
					trainl.setSource(fs.open(file));
				} else trainl.setSource(a.download(filename + ".arff","temp/"));
				trainl.setRetrieval(Loader.BATCH);
				Instances train = trainl.getDataSet();
				train.setClassIndex(train.numAttributes() - 1);
				//creating random forest classifier with ten trees
				RandomForest classifier = new RandomForest();
				classifier.setNumTrees(10);
				try {classifier.buildClassifier(train);}
				catch (Exception e) {LOGGER.error(e.getMessage());}
				train = null;
				fs = FileSystem.get(context.getConfiguration());
				// loading test data
				file = new Path("testtemp/" + filename + ".arff");
				ArffLoader testl = new ArffLoader();
				testl.setSource(fs.open(file));
				testl.setRetrieval(Loader.BATCH);
				Instances test = testl.getDataSet();
				test.setClassIndex(test.numAttributes() - 1);
				doClassification(test, classifier, context);
			} catch (IOException e) {LOGGER.error(e.getMessage());}
		}

		/**
		 * method to classify and get output
		 * 
		 * @param testDataSet
		 *            test data
		 * @param classifier
		 *            random forest classifier
		 * @param context
		 *            to write to output
		 * @throws IOException
		 */
		private void doClassification(Instances test,
				RandomForest classifier, Context context) throws IOException {
			String pred = "";
			double classification;
			for (int i = 0; i < test.numInstances(); i++) {
				Instance instance = test.instance(i);
				try {
					classification = classifier.classifyInstance(instance);
					pred = instance.classAttribute().value((int) classification);
					context.write(new Text(this.okeysp.get(i)), new Text(pred));
				} catch (Exception e) {	LOGGER.error(e.getMessage());}
			}
		}
	}

}
