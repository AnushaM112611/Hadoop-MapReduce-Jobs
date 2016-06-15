package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	String s1 = value.toString();
    	String year = s1.substring(15,19);
    	int temp;
    	
    	if (s1.charAt(87)== '+'){
    		
    		temp = Integer.parseInt(s1.substring(88,92));
    	}else {
    		temp = Integer.parseInt(s1.substring(87,92));
    	}
    	
		
            context.write( new Text(year),new IntWritable(temp));
        
        
        }
    }

