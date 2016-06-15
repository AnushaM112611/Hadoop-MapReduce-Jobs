package com.training.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class NLineMapper extends Mapper<LongWritable, Text, LongWritable,Text> {

    @Override
    public void map(LongWritable key,Text values, Context context) throws IOException, InterruptedException {

            context.write(key,values);


    }
}
