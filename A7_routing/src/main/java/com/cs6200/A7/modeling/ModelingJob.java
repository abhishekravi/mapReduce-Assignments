package com.cs6200.A7.modeling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.cs6200.A7.A7;

/**
 * 
 * @author Chintan Pathak, Chinmayee Vaidya
 *
 */

/**
 * The driver class for the modeling phase of the program
 * This class creates a map reduce job instance using 
 *   - ReadDataMapper as the mapper class
 *   - BuildModelReducer as the reducer class
 *   - Text as the key to pass data between them
 *   - PredictionCompositeValue as the value to pass data between them
 */
public class ModelingJob extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("bucket.name", args[2]);
		
		Job job = Job.getInstance(conf, "Modeling");
		job.setJarByClass(A7.class);
		job.setMapperClass(ReadDataMapper.class);
		job.setReducerClass(BuildModelReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ModelingCompositeValue.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "/output"));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
