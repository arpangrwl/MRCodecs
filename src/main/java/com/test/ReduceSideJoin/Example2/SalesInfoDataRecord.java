package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * Encapsulation of sales information, sales amount as an input.
 * Used as an output value of salesInfo mapper after further encapsulation by JoinGenericWritable.
 * Decapsulation in JoinReducer, then being recorded in intermediate result.
 */

public class SalesInfoDataRecord implements Writable {
	public DoubleWritable sales = new DoubleWritable();
	
	public SalesInfoDataRecord(){}
	
	public SalesInfoDataRecord(double sales) {
		/** Set the value of this DoubleWritable. */
		this.sales.set(sales);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.sales.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.sales.write(out);
	}
	
}
