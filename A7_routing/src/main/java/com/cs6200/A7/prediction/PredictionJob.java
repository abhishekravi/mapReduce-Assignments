package com.cs6200.A7.prediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.cs6200.A7.A7;

/**
 * 
 * @author Chintan Pathak, Chinmayee Vaidya
 *
 */

/**
 * The driver class for the prediction phase of the program
 * This class creates a map reduce job instance using 
 *   - TestDataMapper as the mapper class
 *   - PredictionReducer as the reducer class
 *   - Text as the key to pass data between them
 *   - PredictionCompositeValue as the value to pass data between them
 */
public class PredictionJob extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("bucket.name", args[3]);
		
		Job job = Job.getInstance(conf, "Predicting");
		job.setJarByClass(A7.class);
		job.setMapperClass(TestDataMapper.class);
		job.setReducerClass(PredictionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PredictionCompositeValue.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TestDataMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RequestMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3] + "/predictions"));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
