package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumberOfMapMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	
	
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	String st = "Number of Mappers";
    	//State st2 = new State();

    	Configuration conf = context.getConfiguration();
    	String input = conf.get("mapred.map.tasks");
    	//String input ="Drama";
        //if( st2.genre.equals(s1[2])){
    	
        context.write( new Text(st),new Text(input));
        
        
        }
    }
