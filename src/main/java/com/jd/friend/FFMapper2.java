package com.jd.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-05-29 22:09
 */
public class FFMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();

    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        v.set(split[0]);
        String[] split1 = split[1].split(",");
        for (int i = 0; i < split1.length; i++) {
            for (int j = i + 1; j < split1.length; j++) {
                if (split1[i].compareTo(split1[j]) > 0) {
                    k.set(split1[j] + "-" + split1[i]);
                } else {
                    k.set(split1[i] + "-" + split1[j]);
                }
                context.write(k, v);
            }
        }
    }

}
