package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * Group comparator, with CustomerIdKey passing in, group by customerId.
 * Put same customerId together.
 * Used in Job class.
 */

public class JoinGroupComparator extends WritableComparator {
	public JoinGroupComparator() {
		super(CustomerIdKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		CustomerIdKey first = (CustomerIdKey) a;
		CustomerIdKey second = (CustomerIdKey) b;
		
		return first.cusotmerId.compareTo(second.cusotmerId);
	}
}