package com.test.ReduceSideJoin.Example2;

/**
 * 
 * Statement:
 * We have two CSVs in HDFS.
 * One has customer information <customer_id,name,street,city,state,zip>.  
 * The other has sales information <timestamp,customer_id,sales_price> .  
 * This program implement a mapreduce job that uses reduce-side join to produce a CSV in HDFS with <state,total_sales>.  
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Entrance of the program.
 * Chaining jobs
 * Set two jobs: first job is used to joined sales by customer names, second job is to join sales by state
 */
public class Driver extends Configured implements Tool {

	// Settings for two jobs
	public int run(String[] allArgs) throws Exception {
		
		getConf().set("mapred.textoutputformat.separator", ",");
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
		
		// Set job1
		Job job = new Job(getConf(), "job 1");
		job.setJarByClass(Driver.class); // Set the Jar by finding where a given class came from.
		
		// set input and output format for the job
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// set mapper output key and value(also reducer input)
		job.setMapOutputKeyClass(CustomerIdKey.class);
		job.setMapOutputValueClass(JoinGenericWritable.class);
		
		// set input data
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesInfoDataMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CustomerMapper.class);
		
		// set reducer class comes from
		job.setReducerClass(JoinReducer.class);
		
		// set sorting and grouping class come from
		job.setSortComparatorClass(JoinSortingComparator.class);
		job.setGroupingComparatorClass(JoinGroupComparator.class);
		
		// set output key and value which class
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
		
		job.waitForCompletion(true);
		
		// Set job2
		Job job2 = new Job(getConf(), "job 2");
		job2.setJarByClass(Driver.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job2, new Path(allArgs[2]), TextInputFormat.class, StateMapper.class);
		
		job2.setReducerClass(StateReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(job2, new Path(allArgs[3]));
		
		return job2.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
	}
}
