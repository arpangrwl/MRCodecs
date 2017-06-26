package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * Sort comparator, with CustomerIdKey passing in, sort by customerId.
 * Used in Job class.
 */

public class JoinSortingComparator extends WritableComparator {
	public JoinSortingComparator() {
		super (CustomerIdKey.class, true); //Class<? extends WritableComparable> keyClass, boolean createInstances
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		CustomerIdKey first = (CustomerIdKey) a;
		CustomerIdKey second = (CustomerIdKey) b;
		
		return first.compareTo(second);
	}
}