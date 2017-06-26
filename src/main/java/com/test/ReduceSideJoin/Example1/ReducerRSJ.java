package com.test.ReduceSideJoin.Example1;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerRSJ extends Reducer<CompositeKeyWritableRSJ, Text, NullWritable, Text> {

	StringBuilder reduceValueBuilder = new StringBuilder("");
	NullWritable nullWritableKey = NullWritable.get();
	Text reduceOutputValue = new Text("");
	String strSeparator = ",";



	private StringBuilder buildOutputValue(CompositeKeyWritableRSJ key,StringBuilder reduceValueBuilder, Text value) {

		if (key.getsourceIndex() == 1) {
			reduceValueBuilder.append(key.getjoinKey()).append(strSeparator);
			reduceValueBuilder.append(value.toString()).append(strSeparator);
		

		} else if (key.getsourceIndex() == 2) {


			String arrSalAttributes[] = value.toString().split(",");
			reduceValueBuilder.append(arrSalAttributes[0].toString()).append(strSeparator);
		}
		return reduceValueBuilder;
	}

	@Override
	public void reduce(CompositeKeyWritableRSJ key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		for (Text value : values) {
			buildOutputValue(key, reduceValueBuilder, value);
		}

		if (reduceValueBuilder.length() > 1) {

			reduceValueBuilder.setLength(reduceValueBuilder.length() - 1);
			reduceOutputValue.set(reduceValueBuilder.toString());
			context.write(nullWritableKey, reduceOutputValue);
		} 
		reduceValueBuilder.setLength(0);
		reduceOutputValue.set("");

	}
}
