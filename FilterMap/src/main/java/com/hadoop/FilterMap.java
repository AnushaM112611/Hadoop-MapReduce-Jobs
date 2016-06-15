package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FilterMap extends Configured implements Tool {

	    public int run(String[] args) throws Exception {

	        if (args.length != 3) {
	            System.out.printf(
	                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
	                    .getSimpleName());
	            
	            ToolRunner.printGenericCommandUsage(System.out);
	            return -1;
	        }

	        Job job = new Job(getConf());
	        job.setJarByClass(FilterMap.class);
	        job.setJobName(this.getClass().getName());
	        
	        Configuration conf = job.getConfiguration();
	        conf.set("MovieGenre", args[0]);
	        FileInputFormat.setInputPaths(job, new Path(args[1]));
	        FileOutputFormat.setOutputPath(job, new Path(args[2]));
	        
	        job.setMapperClass(FilterMapMapper.class);
	       // job.setReducerClass(FilterMapReducer.class);

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setNumReduceTasks(0);
	        
	        /*job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);*/

	        if (job.waitForCompletion(true)) {
	            return 0;
	        }
	        return 1;
	    }

	    public static void main(String[] args) throws Exception {
	    	
	    	
	    	
	    	
	    	int exitCode = ToolRunner.run(new FilterMap(), args);
	        System.exit(exitCode);
	        
	    }
	    
	   	}


