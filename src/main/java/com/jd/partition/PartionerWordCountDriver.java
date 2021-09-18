package com.jd.partition;

import com.jd.mapreduce.flow.FlowBean;
import com.jd.mapreduce.flow.FlowMapper;
import com.jd.mapreduce.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author FM
 * @Description
 * @create 2021-05-17 22:45
 */
public class PartionerWordCountDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PartionerWordCountDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setPartitionerClass(MyPartioner.class);
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
