package com.training.distributedcache;

import org.apache.commons.httpclient.URI;
import org.apache.commons.lang.enums.Enum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DistributedCacheDriver extends Configured implements Tool {


    enum counters {

        EXISTS,
        DOESNOTEXIST,
        HASH

    };

    public int run(String[] strings) throws Exception {


        if (strings.length != 3) {
            System.out.printf("Please provide correct number of arguments!!!");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Configuration configuration = new Configuration();

        DistributedCache.addCacheFile((new Path(strings[0])).toUri(),configuration);

        JobConf conf = new JobConf(configuration);
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");

        Job job = new Job(configuration);
        job.setJarByClass(DistributedCacheDriver.class);
        job.setJobName(this.getClass().getName());





        //Set the input adn output format of classes
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //Set input files with corrosponding arguments to file input and output formats
        KeyValueTextInputFormat.setInputPaths(job,new Path(strings[1]));
        FileOutputFormat.setOutputPath(job,new Path(strings[2]));

        //Set the mapper class
        job.setMapperClass(DistributedCacheMapper.class);

        //Set key and value type for mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Set number of reducer to 0 as performing mapper only task.
        job.setNumReduceTasks(0);

        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        if(job.waitForCompletion(true))
        {
            Counters c= job.getCounters();
            System.out.println("File exists count: "+c.findCounter(counters.EXISTS).getValue());
            System.out.println("File not exists count: "+c.findCounter(counters.DOESNOTEXIST).getValue());
            System.out.println("num of records exists count: "+c.findCounter(counters.HASH).getValue());
            return 0;
        }

        return 1;

    }

    public static void main(String[] args) throws Exception {

        int exitcode = ToolRunner.run(new DistributedCacheDriver(),args);
        System.exit(exitcode);
    }

}
