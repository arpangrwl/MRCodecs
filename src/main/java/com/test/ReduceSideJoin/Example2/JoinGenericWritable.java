package com.test.ReduceSideJoin.Example2;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 * 
 * 	A wrapper for Writable instances.
 *  Use as mapper output value, and reducer input value.
 */
public class JoinGenericWritable extends GenericWritable {
	private static Class<? extends Writable>[] CLASSES = null;
	
	// static initialization block
	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] {
                SalesInfoDataRecord.class,
                CustomerRecord.class
        };
	}
	
	public JoinGenericWritable() {}
	
	public JoinGenericWritable(Writable instance) {
		set(instance); // set method in GenericWritable class, set instance that is wrapped.
	}

	// get type of the class
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}
}
