package com.test.customRecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRDriverRecReader extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MRDriverRecReader(), args);
    }

    public int run(String[] args) throws Exception {
        //creating a Job configuration object and assigning a job name for identification purposes
        Job job = new Job(getConf());

      //  Configuration conf = new Configuration();
        //conf.set("fs.default.name","hdfs://17.176.88.191:50001");


        job.setJobName("Word Count Job");
        job.setJarByClass(MRDriverRecReader.class);

        job.setNumReduceTasks(1);
        job.setMapperClass(MRMapperRecReader.class);
        job.setReducerClass(MRReducerRecReader.class);

        job.setInputFormatClass(ExtendedTextInpunFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //Setting configuration object with the Data Type of output Key and Value

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(getConf());

        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        return job.waitForCompletion(true) == true ? 0 : -1;
    }

}