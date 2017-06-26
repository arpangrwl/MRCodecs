package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * Encapsulation of customer information, state as an input.
 * Used as an output value of customer mapper after further encapsulation by JoinGenericWritable.
 * Decapsulation in JoinReducer, then being recorded in intermediate result.
 */

public class CustomerRecord implements Writable {
	public Text state = new Text();
	
	public CustomerRecord() {}
	
	public CustomerRecord(String state) {
		this.state.set(state);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.state.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.state.write(out);
	}
	
}
