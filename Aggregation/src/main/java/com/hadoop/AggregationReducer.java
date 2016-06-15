package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AggregationReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
    	
    	Configuration conf = context.getConfiguration();
    	String input = conf.get("Opt");
    	int temp=0;
    	int count=0;
       
       if (input.equals("Average")){
    	   for (IntWritable value : values){
    		   temp = temp+value.get();
    		   count++;
    	   }
    	   int average=temp/count;
    	   context.write(key, new Text("Average Temperature :"+average));
       }else if (input.equals("Count")){
    	   for (IntWritable value : values){
    		  
    		   count++;
    	   }
    	   context.write(key, new Text("Total count of Temperatures :"+count));
       }else if (input.equals("Max")){
    	   
    	   int max = Integer.MIN_VALUE;
    	   
    	   for(IntWritable value :values){
    		   max = Math.max(max, value.get());
    	   }
    	   
    	   context.write(key, new Text("Maximum Temperature :" +max));  	   
       }
       
  
    }
}






