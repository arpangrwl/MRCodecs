package com.test.SecondarySort.Example1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PersonReducer extends Reducer<Person, Text, Text, Text>{

	public void reduce(Person key,Iterable<Text> value, Context context) throws IOException, InterruptedException{
        for(Text fname:value){
		context.write(key.getLname(), fname);
	}

}

}