package com.hadoop.innerjoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class DeptMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	protected void map(LongWritable offset, Text record, Context context)
			throws IOException, InterruptedException {
		String[] row = record.toString().split(",");
		context.write(new TextPair(row[0], "0"), new Text(row[1]));

	}

}
