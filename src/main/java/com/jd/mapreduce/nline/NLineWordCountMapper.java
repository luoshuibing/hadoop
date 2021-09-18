package com.jd.mapreduce.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-17 22:25
 */
public class NLineWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();

   IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
