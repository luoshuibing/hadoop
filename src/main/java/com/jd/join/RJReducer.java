package com.jd.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 20:51
 */
public class RJReducer extends Reducer<RJOrderBean, NullWritable, RJOrderBean, NullWritable> {

    @Override
    protected void reduce(RJOrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getPname();
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
