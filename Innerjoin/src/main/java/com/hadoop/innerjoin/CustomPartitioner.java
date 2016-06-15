package com.hadoop.innerjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class CustomPartitioner extends Partitioner<TextPair, Text> {

	
	public int getPartition(TextPair compositeKey, Text record, int numberOfPartitions) {
		
		return (compositeKey.getFirst().hashCode()%numberOfPartitions);
	}

	
	

	
	

}
