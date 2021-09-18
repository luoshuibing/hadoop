package com.jd.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-26 23:55
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    // private FileOutputStream baidu;
    //
    // private FileOutputStream other;

    private FSDataOutputStream baidu;

    private FSDataOutputStream other;

    public MyRecordWriter(TaskAttemptContext job) {
        try {
            init(job);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void init(TaskAttemptContext job) throws Exception {
        String outDir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        baidu=fileSystem.create(new Path(outDir + "/baidu.log"));
        other=fileSystem.create(new Path(outDir + "/other.log"));
    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String out = value.toString() + "\n";
        if (out.contains("baidu")) {
            baidu.write(out.getBytes());
        } else {
            other.write(out.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(baidu);
        IOUtils.closeStream(other);
    }
}
