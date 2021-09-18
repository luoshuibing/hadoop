package com.jd.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 21:26
 */
public class IndexMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);
    private String fileName;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        this.fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        for (String field : fields) {
            k.set(field + "--" + fileName);
            context.write(k, v);
        }
    }
}
