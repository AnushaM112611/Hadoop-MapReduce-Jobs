package com.hadoop.findunique;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UniqueReducer extends Reducer<Text , Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Set<String> s =new HashSet<String>();
        for(Text ipvalue: values){

            s.add(ipvalue.toString());
        }
        //to know number of ipaddress
        int ipcount = s.size();

        context.write(key,new IntWritable(ipcount));

    }

}
