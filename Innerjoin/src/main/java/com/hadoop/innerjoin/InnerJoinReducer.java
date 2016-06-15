package com.hadoop.innerjoin;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class InnerJoinReducer extends
		Reducer<TextPair, Text, NullWritable, Text> {

	protected void reduce(TextPair compositeKey, Iterable<Text> rows,
			Context context) throws IOException, InterruptedException {

		String deptName = null;
		boolean set = false;

		for (Text row : rows) {
			if (!set) {
				deptName = row.toString().split(",")[0];
				set = true;
			} else {
				String[] record = row.toString().split(",");
				context.write(null, new Text(record[0] + "," + record[1] + ","
						+ deptName));
			}

		}
	}

}
