package com.test.AvroReadWordCount;


import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AvroMRMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, LongWritable> {

    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {

        try {

            if (value != null) {

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
