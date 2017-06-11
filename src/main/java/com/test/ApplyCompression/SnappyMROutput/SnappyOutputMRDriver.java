package com.test.ApplyCompression.SnappyMROutput;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SnappyOutputMRDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new SnappyOutputMRDriver(), args);
    }

    public int run(String[] args) throws Exception {
        //creating a Job configuration object and assigning a job name for identification purposes

        Configuration conf = getConf();

        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("io.compression.codecs","org.apache.hadoop.io.compress.SnappyCodec");
        // Compress MapReduce output - snappy
        conf.set("mapred.output.compress", "true");
        conf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("output.compression.enabled", "true");
        conf.set("output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");


        Job job = new Job(conf);
        job.setJobName("Word Count Job");
        job.setJarByClass(SnappyOutputMRDriver.class);

        job.setNumReduceTasks(1);
        job.setMapperClass(com.test.MRWordCount.MRMapper.class);
        job.setReducerClass(com.test.MRWordCount.MRReducer.class);
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Setting configuration object with the Data Type of output Key and Value

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        return job.waitForCompletion(true) == true ? 0 : -1;
    }

}