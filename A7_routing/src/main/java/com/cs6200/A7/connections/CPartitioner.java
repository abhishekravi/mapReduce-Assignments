package com.cs6200.A7.connections;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Used to partition the data into the correct reducer(needed since secondary sort is being used).
 * @author Abhishek Ravi Chandran, Mania Abdi
 */
public class CPartitioner extends Partitioner<CustomKey, CWritable> {
    
    @Override
    public int getPartition(CustomKey key, CWritable val, int numPartitions) {
		int hash = key.cyear.hashCode();
        return Math.abs(hash % numPartitions);
    }
 
}
