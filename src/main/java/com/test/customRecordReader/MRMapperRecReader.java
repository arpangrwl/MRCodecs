package com.test.customRecordReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MRMapperRecReader extends Mapper<Text, IntWritable, Text, IntWritable> {
    //hadoop supported data types

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        //taking one line at a time and tokenizing the same
        context.write(key, value);
        }
    }