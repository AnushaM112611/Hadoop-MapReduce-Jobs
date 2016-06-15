package com.hadoop.delimiter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class DelimiterMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] s = value.toString().split(",");

            context.write(key, new Text(s[0]+" "+s[1]+" "+s[2]));
    }

}
