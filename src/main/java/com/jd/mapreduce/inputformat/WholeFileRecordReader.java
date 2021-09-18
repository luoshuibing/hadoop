package com.jd.mapreduce.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-24 22:35
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notReadFlag = true;

    private Text key = new Text();

    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;

    private FileSplit fs;

    /**
     * 初始化方法，框架在开始的时候调用一次
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fs=(FileSplit)split;
        Path path = fs.getPath();
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        inputStream = fileSystem.open(path);
    }

    /**
     * 读取下一组K,V
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(notReadFlag){
            //读文件
            key.set(fs.getPath().toString());
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf,0,buf.length);
            notReadFlag=false;
            return true;
        }
        return false;
    }

    /**
     * 获取当前获取到的key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 当前数据读取状态
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notReadFlag ? 0 : 1;
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }

}
