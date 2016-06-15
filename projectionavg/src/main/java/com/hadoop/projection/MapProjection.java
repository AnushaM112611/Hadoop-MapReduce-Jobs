package com.hadoop.projection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapProjection extends Mapper<LongWritable, Text,IntWritable,Text > {

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String[] line = values.toString().split(",");
        String details= "";
        int sid;

        sid = Integer.parseInt(line[0]);
        details = line[1]+","+line[3]+","+line[5];

        context.write(new IntWritable(sid),new Text(details));


    }

}
