package com.hadoop.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class JoinMapper extends Mapper<LongWritable,Text,Text,Text>
{
    Text keyEmit = new Text();
    Text valEmit = new Text();
    public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
    {
        String line=value.toString();
        String[] words=line.split(",");

        context.write(new Text(words[2]), new Text (words[0]+","+words[1]+"s1"));
    }
}

