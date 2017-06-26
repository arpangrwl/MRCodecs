package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 
 * Mapper for sales_info table.
 * Input pair: LongWritable-no use, Text-raw data of this id
 * Output pair: CustomerIdKey-encapsulated customer info, JoinGenericWritable-double encapsulated sales info
 * Next step: JoinReducer.java
 */

public class SalesInfoDataMapper extends Mapper<LongWritable, Text, CustomerIdKey, JoinGenericWritable> {
	//<customerId, value, >
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] recordFields = value.toString().split(",");
		
		//get customer id
		int customerId = Integer.parseInt(recordFields[1]);
		
		//get amount of sales
		double sales = Double.parseDouble(recordFields[2]);
		
		CustomerIdKey recordKey = new CustomerIdKey(customerId, CustomerIdKey.DATA_RECORD); //output key
		SalesInfoDataRecord record = new SalesInfoDataRecord(sales); // output value before encapsulation
		
		// encapsulate output value
		JoinGenericWritable genericRecord = new JoinGenericWritable(record); //output value
		
		// output key-value pair, write into mapper context
		context.write(recordKey, genericRecord);
	}
}