package com.hadoop;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;




public class CustomPartitioner extends Partitioner<Text, IntWritable> {

	
	public int getPartition(Text key, IntWritable record, int numberOfPartitions) {
		
				String s = key.toString();	
				int key2 = 0;
				
				char c = s.charAt(0);
				
				if (c>='a' && c <= 'j'){
					key2=0;
				} else if (c>='k' && c <= 't'){
					key2=1;
				} else if (c>='t'){
					key2=2;
				}
				
				return key2%numberOfPartitions;
		
	}

	
	

	
	

}
