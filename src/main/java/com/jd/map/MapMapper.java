package com.jd.map;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author FM
 * @Description
 * @create 2021-05-27 21:37
 */
public class MapMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pMap = new HashMap<>();

    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(uris[0])));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split("\t");
            pMap.put(split[0], split[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        split[1] = pMap.get(split[1]);
        k.set(split[0] + "\t" + split[1] + "\t" + split[2]);
        context.write(k, NullWritable.get());
    }
}
