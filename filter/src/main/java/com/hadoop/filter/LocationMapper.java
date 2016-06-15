package com.hadoop.filter;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;



public class LocationMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    public void map(LongWritable key,Text values, Context context) throws IOException, InterruptedException {


        String s = values.toString();
        String[] line = s.split(",");

            if(line[2].equalsIgnoreCase("lehi")){
                context.write(new Text(line[2]),new Text(line[1]+" --> "+line[3]));
            }

        }

}


