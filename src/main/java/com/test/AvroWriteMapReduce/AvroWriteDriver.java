package com.test.AvroWriteMapReduce;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroWriteDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AvroWriteDriver(), args);
    }

    public int run(String[] args) throws Exception {
        //creating a Job configuration object and assigning a job name for identification purposes
        Job job = new Job(getConf());
        job.setJobName("Word Count Job");
        job.setJarByClass(AvroWriteDriver.class);

        job.setNumReduceTasks(1);
        job.setMapperClass(AvroWriteMapper.class);
        job.setReducerClass(AvroWriteReducer.class);
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //Setting configuration object with the Data Type of output Key and Value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        AvroJob.setOutputKeySchema(job, WordCountSchema.getClassSchema());
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        return job.waitForCompletion(true) == true ? 0 : 1;
    }

}