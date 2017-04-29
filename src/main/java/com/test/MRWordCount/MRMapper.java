package com.test.MRWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MRMapper extends Mapper<Object, Text, Text, IntWritable> {
    //hadoop supported data types
	private Text word = new Text();
	//private final static LongWritable one = new LongWritable(1);

	@Override
    //map method that performs the tokenizer job and framing the initial key value pairs
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //taking one line at a time and tokenizing the same
			String line = value.toString();
			String strTocken[] = line.split("\\s+");
	      //iterating through all the words available in that line and forming the key value pair

		for(String s :strTocken)
		{
			word.set(s);
			context.write(word, new IntWritable(1));
		}
	}
}