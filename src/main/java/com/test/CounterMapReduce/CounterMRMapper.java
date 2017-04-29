package com.test.CounterMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CounterMRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    //map method that performs the tokenizer job and framing the initial key value pairs
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //taking one line at a time and tokenizing the same
        String line = value.toString();
        String strTocken[] = line.split("\\s+");
        //iterating through all the words available in that line and forming the key value pair

        for (String s : strTocken) {
            context.write(new Text(s), new IntWritable(1));
            System.out.println("Print the value also " + s);
        }
    }
}