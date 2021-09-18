package com.jd.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-22 23:42
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    private FlowBean flowBean=new FlowBean();

    /**
     *
     * @param key Map中输出的结果
     * @param values    Map中输出的结果
     * @param context   上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow=0;
        long sumDownFlow=0;
        for (FlowBean value : values) {
            sumUpFlow+=value.getUpflow();
            sumDownFlow+=value.getDownflow();
        }
        flowBean.set(sumUpFlow,sumDownFlow);
        context.write(key,flowBean);
    }
}

