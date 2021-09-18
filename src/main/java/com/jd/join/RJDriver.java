package com.jd.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 20:52
 */
public class RJDriver {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(RJDriver.class);
        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);

        job.setMapOutputKeyClass(RJOrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(RJOrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(RJComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
