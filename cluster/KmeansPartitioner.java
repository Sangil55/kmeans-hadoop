package com.swinno.hadoop.cluster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KmeansPartitioner extends Partitioner<IntWritable, DoubleArrayWritable> {

	@Override
	public int getPartition(IntWritable key, DoubleArrayWritable value, int numReduceTasks) {
		
		return Math.abs((key.get()/1000)%numReduceTasks);
	}

}
