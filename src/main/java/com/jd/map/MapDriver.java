package com.jd.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 21:36
 */
public class MapDriver {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MapDriver.class);
        job.setMapperClass(MapMapper.class);

        job.setNumReduceTasks(0);

        job.addCacheFile(URI.create("file:///E:/data/input/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
