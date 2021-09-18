package com.jd.mapreduce.keyvalue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-24 21:56
 */
public class KeyValueMapper extends Mapper<Text,Text,Text, IntWritable> {

    private IntWritable v=new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,v);
    }
}
