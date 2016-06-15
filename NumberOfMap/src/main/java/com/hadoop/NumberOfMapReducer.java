package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NumberOfMapReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
       
       String input = null;
       
        for (Text value : values) {
            input = value.toString();
        
        break;
        }
        
        context.write(key, new Text(input));
    }
}






