package com.test.customRecordReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by Arpan on 6/8/17.
 */
public class ExtendedTextInpunFormat extends FileInputFormat<Text, IntWritable> {

    ExtendedTextInpunFormat(){

    }

    public RecordReader<Text, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if(null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes();
        }

        return new ExtendLineRecordReader(recordDelimiterBytes);
    }

    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = (new CompressionCodecFactory(context.getConfiguration())).getCodec(file);
        return codec == null;
    }
}
