package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 *  Use customer ID as a foreign key to manipulate map and reduce.
 *  Encapsulate customer ID information into this class.
 *  Set flag to distinguish which table current data comes from.
 *  This class is the output in mapper, input in reducer.
 */

public class CustomerIdKey implements WritableComparable<CustomerIdKey> {

	// set flag value to tell the source of the data, like composite key in database
	public static final IntWritable CUSTOMER_RECORD = new IntWritable(0);
	public static final IntWritable DATA_RECORD = new IntWritable(1);
	
	public IntWritable cusotmerId = new IntWritable();
	public IntWritable recordType = new IntWritable(); // set recordType as a flag
	
	public CustomerIdKey() {}
	
	public CustomerIdKey(int customerId, IntWritable recordType) {
		this.cusotmerId.set(customerId);
		this.recordType = recordType;
	}

	public void write(DataOutput out) throws IOException {
		this.cusotmerId.write(out);
		this.recordType.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.cusotmerId.readFields(in);
		this.recordType.readFields(in);
	}
	
	/**	
	 * 	If the instances are from same table, then group(split and map);
	 *  If not same table, then sort(shuffle and reduce).
	 * 	After sorting, the instances should be sorted by their customerId. 
	 * 	Instances from customer_info table should be in front of that from sales_info table.
	 * 	For example : 123(c), 123(s), 234(c), 234(s), 234(s)
	 */
	public int compareTo(CustomerIdKey o) {
		if (this.cusotmerId.equals(o.cusotmerId)) { // group
			return this.recordType.compareTo(o.recordType);
		} else {
			return this.cusotmerId.compareTo(o.cusotmerId); //sort
		}
	}
}
