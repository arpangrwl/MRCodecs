package com.test.ReduceSideJoin.Example1;

//********************************************************************************
//Class:    PartitionerRSJ 
//Purpose:  Custom partitioner 
//Author:   Anagha Khanolkar
//*********************************************************************************



import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerRSJ extends Partitioner<CompositeKeyWritableRSJ, Text> {

	@Override
	public int getPartition(CompositeKeyWritableRSJ key, Text value,
			int numReduceTasks) {
		return (key.getjoinKey().hashCode() % numReduceTasks);
	}
}
