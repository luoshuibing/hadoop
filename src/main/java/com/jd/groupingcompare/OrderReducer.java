package com.jd.groupingcompare;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-26 21:52
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }

}
