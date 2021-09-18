package com.jd.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 20:30
 */
public class TopNReducer extends Reducer<TopNFlowBean, Text, Text,TopNFlowBean> {

    @Override
    protected void reduce(TopNFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        for(int i=0;i<10;i++){
            context.write(iterator.next(),key);
        }
    }
}
