package com.test.SecondarySort.Example1;/*#############################################################################################
# Description: SecondarySort using Map Reduce
#
# Input: 
#   1. /user/cloudera/MarkTwain.txt
#
# To Run this code use the command:    
# yarn jar MRSecondarySort-0.0.1-SNAPSHOT.jar \
#		   com.stdatalabs.MRSecondarySort.SecondarySortDriver \
#		   fName_lName.csv \
#		   MRSecondarySort_op
#
#
#	Debugging in Local :-
#	~/MRCodecs/src/main/resources/Names.csv  /outputPath_Directory
#
#############################################################################################*/

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {

	public static void main(String args[]) throws Exception {
        ToolRunner.run(new SecondarySortDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SecondarySortDriver.class);
		
		job.getConfiguration().set("key.value.separator.in.input.line", ",");
		//job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		job.setMapperClass(PersonMapper.class);
		job.setReducerClass(PersonReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(PersonPartitioner.class);
		job.setSortComparatorClass(PersonSortingComparator.class);
		job.setGroupingComparatorClass(PersonGroupingComparator.class);

		job.setNumReduceTasks(2);
		
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		job.waitForCompletion(true);
		return 0;
	}

}
