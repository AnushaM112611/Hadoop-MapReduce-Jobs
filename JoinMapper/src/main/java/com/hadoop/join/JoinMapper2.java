package com.hadoop.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public  class JoinMapper2 extends Mapper<LongWritable,Text,Text,Text>
{
    Text keyEmit = new Text();
    Text valEmit = new Text();
    public void map(LongWritable k, Text v, Mapper.Context context) throws IOException, InterruptedException
    {
        String line=v.toString();
        String[] words=line.split(",");

        context.write(new Text(words[0]), new Text (words[1]+"s2"));
    }
}
