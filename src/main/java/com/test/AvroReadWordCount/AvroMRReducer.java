package com.test.AvroReadWordCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvroMRReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    long totalMarks =0;

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        for(LongWritable value : values) {
            totalMarks= totalMarks + value.get() ;
        }

        context.write(key, new LongWritable(totalMarks));
    }
}

