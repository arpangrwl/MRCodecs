package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 
 * Mapper for last Job, group data by states.
 * Input pair: Object-no use, Text-output Text from JoinReducer.
 * Output pair: Text-states, DoubleWritable-sales.
 * Next step: StateReducer.java
 */

public class StateMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(", ");
		
		// filter invalid information
		if(fields.length != 3) return;
		
		//get state
		Text state = new Text(fields[1]);
		
		//get sales
		DoubleWritable sales = new DoubleWritable(Double.valueOf(fields[2].toString()));
		
		// output key-value pair, write into mapper context
		context.write(state, sales);
	}
}
