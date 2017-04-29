package com.test.CounterMapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Iterator;

public class CounterMRDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CounterMRDriver(), args);
    }

    public int run(String[] args) throws Exception {
        //creating a Job configuration object and assigning a job name for identification purposes
        Job wordCountJob = new Job(getConf());
        wordCountJob.setJobName("Word Count Job");
        wordCountJob.setJarByClass(this.getClass());

        wordCountJob.setNumReduceTasks(1);

        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(wordCountJob, new Path(args[0]));
        //FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));

        //Setting configuration object with the Data Type of output Key and Value
        wordCountJob.setMapperClass(CounterMRMapper.class);
        wordCountJob.setReducerClass(CounterMRReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        wordCountJob.setInputFormatClass(TextInputFormat.class);
        wordCountJob.setOutputFormatClass(NullOutputFormat.class);


        wordCountJob.waitForCompletion(true);
        Iterator<Counter> itr = wordCountJob.getCounters().getGroup("VAL").iterator();

        Counter ctr;
        while (itr.hasNext()) {
            ctr = itr.next();
            System.out.println(ctr.getName() + " ---> " + ctr.getValue());
        }

        return 0;
    }
}