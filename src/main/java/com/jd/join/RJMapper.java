package com.jd.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 20:51
 */
public class RJMapper extends Mapper<LongWritable, Text, RJOrderBean, NullWritable> {

    private RJOrderBean rj = new RJOrderBean();

    private String fileName;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fileName.startsWith("order")) {
            rj.setId(fields[0]);
            rj.setPid(fields[1]);
            rj.setAmount(Integer.valueOf(fields[2]));
            rj.setPname("");
        } else {
            rj.setPid(fields[0]);
            rj.setPname(fields[1]);
            rj.setId("");
            rj.setAmount(0);
        }
        context.write(rj, NullWritable.get());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        this.fileName = fs.getPath().getName();
    }
}
