package com.jd.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 22:09
 */
public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();

    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        v.set(split[0]);
        for (String s : split[1].split(",")) {
            k.set(s);
            context.write(k,v);
        }
    }
}
