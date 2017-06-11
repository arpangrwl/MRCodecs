package com.test.AvroReadWordCount;


import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class AvroMRMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, LongWritable> {

    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {

        try {

            if (value != null) {
                Path filePath = ((FileSplit) context.getInputSplit()).getPath();
                String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();


                String StudentName = (String) key.datum().get("targetTopic");
                int Marks = (Integer) key.datum().get("partition");


                context.write(new Text(StudentName), new LongWritable(Marks));

            } else {
                System.out.println("The value is not null.. This should have not happened..");

            }

        } catch (Exception exception) {

            System.out.println("*********Exception while data parsing *********");
            exception.printStackTrace();
        }
    }

}
