package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 
 * First Reducer. Receive from CustomerMapper and SalesInfoDataMapper, integrate by customerId.
 * Input pair: CustomerIdKey-from two mappers, JoinGenericWritable-other information(including states and sales).
 * Output pair: NullWritable-no keys needed, Text-states and sales information each customerId.
 * Be prepared for next Job: integrate by states.
 * Next step: StateMapper.java
 */

public class JoinReducer extends Reducer<CustomerIdKey, JoinGenericWritable, NullWritable, Text> {
	@Override
	protected void reduce(CustomerIdKey key, Iterable<JoinGenericWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		// Use Stringbuilder to append customerId
		StringBuilder output = new StringBuilder();
		
		// sumSales for total sales
		double sumSales = 0.0;
		
		/*
		 * Go through the value's each record with this customerId.
		 * If this information is from customer_info table, append the record in stringbuilder.
		 * If from sales_info table, sum up the sales
		 */
		for (JoinGenericWritable value : values) {
			Writable record = value.get();
			
			if (key.recordType.equals(CustomerIdKey.CUSTOMER_RECORD)) { // if value is from customer_info
				CustomerRecord customerRecord = (CustomerRecord) record;
				output.append(Integer.parseInt(key.cusotmerId.toString())).append(", ");
				output.append(customerRecord.state.toString()).append(", ");
			} else { // if value is from sales_info
				SalesInfoDataRecord record2 = (SalesInfoDataRecord) record;
				sumSales += Double.parseDouble(record2.sales.toString());
			}
		}
		
		// output key-value pair, write into reducer context
		context.write(NullWritable.get(), new Text(output.toString() + sumSales));
	}
}