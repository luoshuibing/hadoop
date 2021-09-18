package com.jd.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 22:10
 */
public class FFDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration1 = new Configuration();
        Job job1 = Job.getInstance(configuration1);
        job1.setJarByClass(FFDriver.class);
        job1.setMapperClass(FFMapper1.class);
        job1.setReducerClass(FFReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);


        Configuration configuration2 = new Configuration();
        Job job2 = Job.getInstance(configuration2);
        job2.setJarByClass(FFDriver.class);
        job2.setMapperClass(FFMapper2.class);
        job2.setReducerClass(FFReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("E:\\data\\output"));
        FileOutputFormat.setOutputPath(job2, new Path("E:\\data\\output2"));
        job2.waitForCompletion(true);


    }

}
