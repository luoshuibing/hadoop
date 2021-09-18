package com.jd.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 21:27
 */
public class IndexDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job1 = Job.getInstance(configuration);
        job1.setJarByClass(IndexDriver.class);
        job1.setMapperClass(IndexMapper1.class);
        job1.setReducerClass(IndexReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean result = job1.waitForCompletion(true);


        Configuration configuration2 = new Configuration();
        Job job2 = Job.getInstance(configuration2);
        job2.setJarByClass(IndexDriver.class);
        job2.setMapperClass(IndexMapper2.class);
        job2.setReducerClass(IndexReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("E:\\data\\output"));
        FileOutputFormat.setOutputPath(job2, new Path("E:\\data\\output1"));
        job2.waitForCompletion(true);


    }

}
