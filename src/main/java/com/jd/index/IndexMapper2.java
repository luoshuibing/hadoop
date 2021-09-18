package com.jd.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 21:26
 */
public class IndexMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();

    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("--");
        k.set(fields[0]);
        String[] split = fields[1].split("\t");
        v.set(split[0]+"-->"+split[1]);
        context.write(k,v);
    }
}
