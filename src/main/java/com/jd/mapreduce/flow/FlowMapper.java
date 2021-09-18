package com.jd.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-22 23:35
 */
public class FlowMapper extends Mapper<LongWritable,Text, Text,FlowBean> {

    private Text k = new Text();

    /**
     *
     * @param key   文件中数据的格式
     * @param value 一行数据
     * @param context   上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phone = fields[1];
        Long upFlow = Long.valueOf(fields[6]);
        Long downFlow = Long.valueOf(fields[7]);
        FlowBean flowBean = new FlowBean();
        flowBean.set(upFlow,downFlow);
        k.set(phone);
        context.write(k,flowBean);
    }
}
