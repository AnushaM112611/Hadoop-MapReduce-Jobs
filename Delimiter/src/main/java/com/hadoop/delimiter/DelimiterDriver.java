package com.hadoop.delimiter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DelimiterDriver extends Configured implements Tool {
    public int run(String[] strings) throws Exception {

        if (strings.length != 2) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                            .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Configuration configuration = new Configuration();
        JobConf conf = new JobConf(configuration);
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");


        Job job = new Job(conf);
        job.setJarByClass(DelimiterDriver.class);
        job.setJobName(this.getClass().getName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        KeyValueTextInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        job.setMapperClass(DelimiterMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

         if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;

}

    public static void main(String[] args) throws Exception {

            int exitCode = ToolRunner.run(new DelimiterDriver(), args);
            System.exit(exitCode);

     }
}
