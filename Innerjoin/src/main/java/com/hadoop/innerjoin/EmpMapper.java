package com.hadoop.innerjoin;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	public void map(LongWritable offset, Text record, Context context)
			throws IOException, InterruptedException {
		String[] row = record.toString().split(",");
		if (!row[7].isEmpty()) {

			context.write(new TextPair(row[7],"1"), new Text(row[0] +","+ row[1]));
		}

	}
}
