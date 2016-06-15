package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	String s1[] = value.toString().split(",");
    	//State st2 = new State();
    	
    	//String input ="Drama";
        //if( st2.genre.equals(s1[2])){
    	
    	Configuration conf = context.getConfiguration();
    	String input = conf.get("MovieGenre");
    	
		if(input.equals(s1[2])){
            context.write( new Text(s1[1]),new Text(s1[0]+" "+s1[1]+" "+s1[2]));
        }
        
        }
    }

