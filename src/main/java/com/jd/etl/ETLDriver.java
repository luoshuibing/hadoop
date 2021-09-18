package com.jd.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 22:16
 */
public class ETLDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //map-reduce中间启用压缩
        // configuration.setBoolean("mapreduce.map.output.compress",true);
        // configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //为了演示压缩过程，注释掉这段代码
        //job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //reducer之后设置压缩
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
