package com.test.AvroReadWordCount;

import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AvroMRDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AvroMRDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        if (args.length < 1) {

            System.err.println("Hadoop jar command must have one argument.");
            System.err.println("args[0] is input file path of producer_offsets data file.");
            return 1;
        }

        String inputPath = args[0];

        Job job = new Job(getConf());
        job.setJobName("Avro Reader Writer");
        job.setJarByClass(AvroMRDriver.class);

        job.setNumReduceTasks(10);

        job.setMapperClass(AvroMRMapper.class);
        job.setReducerClass(AvroMRReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroKeyInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(getConf());

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        return job.waitForCompletion(true) == true ? 0 : -1;
    }
}
