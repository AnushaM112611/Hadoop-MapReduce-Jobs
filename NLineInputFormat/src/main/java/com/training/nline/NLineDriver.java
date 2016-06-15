package com.training.nline;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NLineDriver extends Configured implements Tool{


    public int run(String[] args) throws Exception {

        Job job = new Job(getConf());
        job.setJarByClass(NLineMapper.class);
        job.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(NLineInputFormat.class);
        job.getConfiguration().set("mapreduce.input.lineinputformat.linespermap","10");

        job.setMapperClass(NLineMapper.class);
        //job.setReducerClass(MapReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

       // job.setOutputKeyClass(Text.class);
       // job.setOutputValueClass(IntWritable.class);

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
        public static void main(String[] args) throws Exception {

            int exitCode = ToolRunner.run(new NLineDriver(), args);
            System.exit(exitCode);


    }
}
