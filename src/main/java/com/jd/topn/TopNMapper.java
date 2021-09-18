package com.jd.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 20:30
 */
public class TopNMapper extends Mapper<LongWritable, Text, TopNFlowBean, Text> {

    private TopNFlowBean topNFlowBean = new TopNFlowBean();
    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        phone.set(fields[0]);
        topNFlowBean.setDownFlow(Long.valueOf(fields[1]));
        topNFlowBean.setUpFlow(Long.valueOf(fields[2]));
        topNFlowBean.setSumFlow(Long.valueOf(fields[3]));
        context.write(topNFlowBean, phone);
    }

}
