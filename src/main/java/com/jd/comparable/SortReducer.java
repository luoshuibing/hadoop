package com.jd.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-26 20:59
 */
public class SortReducer extends Reducer<CompareFlowBean, Text,Text,CompareFlowBean> {

    @Override
    protected void reduce(CompareFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
