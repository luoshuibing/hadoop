package com.jd.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-26 20:54
 */
public class SortMapper extends Mapper<LongWritable, Text, CompareFlowBean, Text> {

    private CompareFlowBean bean=new CompareFlowBean();

    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        phone.set(fields[0]);
        bean.set(Long.valueOf(fields[1]),Long.valueOf(fields[2]));
        context.write(bean,phone);
    }
}
