package com.training.distributedcache;

import org.apache.avro.JsonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class DistributedCacheMapper extends Mapper<Text, Text,Text, Text> {

    URI[] urls;
    Path[] p1;
    HashMap<String,String> hm = new HashMap<String, String>();
    static boolean x = true;

    @Override
    public void setup(Mapper<Text, Text,Text, Text>.Context context) throws IOException {

       // Configuration conf = JobConf(job);
        //Configuration configuration = new Configuration();
        //JobConf conf = new JobConf();
        p1  = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        FileSystem fs = FileSystem.getLocal(context.getConfiguration());
        if(fs.exists(p1[0])) {
            context.getCounter(DistributedCacheDriver.counters.EXISTS).increment(1);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p1[0])));
            String line = "";
            while ((line =br.readLine()) != null) {


                String[] lines = line.split(",");
                 hm.put(lines[0], lines[1]);

            }
           
        }
       
     }


    @Override
    public void map(Text key, Text values, Context context) throws IOException, InterruptedException {

        String[] line = values.toString().split("|");
        String ip_address = key.toString();

        if(hm.containsKey(ip_address)){

                context.write(new Text(ip_address),new Text(hm.get(ip_address).toString()));
         }

       

     }
}
