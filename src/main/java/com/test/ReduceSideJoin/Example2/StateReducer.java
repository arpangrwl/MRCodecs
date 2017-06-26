package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 
 * Second Reducer. Receive from StateMapper, integrate by state.
 * Input pair: Text-states, DoubleWritable-sales.
 * Output pair: Text-states, DoubleWritable-sales.
 * Final output.
 */

public class StateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		double sum = 0.0;
		
		for (DoubleWritable val : values) {
			sum += val.get();
		}
		
		// output key-value pair, write into reducer context, as the final result
		context.write(key, new DoubleWritable(sum));
	}
}