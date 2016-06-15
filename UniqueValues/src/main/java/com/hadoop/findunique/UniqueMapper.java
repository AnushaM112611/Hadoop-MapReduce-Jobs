package com.hadoop.findunique;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;


public class UniqueMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String s = value.toString();
        StringTokenizer str = new StringTokenizer(s,",");
        //skip first column
        str.nextToken();
        //store second field in web
        String web = str.nextToken();
        String ip = str.nextToken();

        context.write(new Text(web),new Text(ip));

    }

}
