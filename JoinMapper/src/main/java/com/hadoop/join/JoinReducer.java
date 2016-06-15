package com.hadoop.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class JoinReducer extends Reducer<Text,Text,Text,Text>
{
    Text valEmit = new Text();
    String merge = "";
    String tmp = "";
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException , InterruptedException
    {
        int i =0;
        for(Text value:values)
        {
                if(!value.toString().equals("")) {
                    tmp = value.toString().replace("s1", "");
                    tmp = tmp.replace("s2", "");
                }
            merge += tmp;

        }
        valEmit.set(merge);
        context.write(key, valEmit);
    }
}