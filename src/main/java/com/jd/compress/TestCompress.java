package com.jd.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * 报错的压缩和解压缩，不知道原因java.io.IOException: unexpected end of stream
 * @author FM
 * @Description
 * @create 2021-05-28 22:18
 */
public class TestCompress {

    public static void main(String[] args) throws Exception {
        // compress("E:\\data\\input\\web.log", BZip2Codec.class);
        decompress("E:\\data\\input\\web.log.bz2");
    }

    private static void decompress(String path) throws Exception {
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(path));
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(path));
        FileOutputStream fos = new FileOutputStream(path.substring(0, path.length() - 4));
        IOUtils.copyBytes(cis, fos,1024 );
        // IOUtils.closeStream(cis);
        // IOUtils.closeStream(fos);
    }

    private static void compress(String path, Class<BZip2Codec> bZip2CodecClass) throws Exception {
        FileInputStream fis = new FileInputStream(path);
        CompressionCodec codec = ReflectionUtils.newInstance(bZip2CodecClass, new Configuration());
        FileOutputStream fos = new FileOutputStream(path + codec.getDefaultExtension());
        CompressionOutputStream outputStream = codec.createOutputStream(fos);
        IOUtils.copyBytes(fis, outputStream, 1024);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }

}
