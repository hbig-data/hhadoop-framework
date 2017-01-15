package com.ryan.hadoop.hdfs.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * 文件压缩
 *
 * @author Rayn on 2017/1/15.
 * @email liuwei412552703@163.com.
 */
public class HdfsFileCompress {

    /**
     * 压缩文件
     *
     * @param codecClassName
     * @throws Exception
     */
    public static void compress(String codecClassName) throws Exception {
        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        //指定要被压缩的文件路径
        FSDataInputStream in = fs.open(new Path("/user/hadoop/aa.txt"));

        //指定压缩文件路径
        FSDataOutputStream outputStream = fs.create(new Path("/user/hadoop/text.gz"));
        //创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);

        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    /**
     * 解压缩
     *
     * @param fileName
     * @throws Exception
     */
    public static void uncompress(String fileName) throws Exception {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FSDataInputStream inputStream = fs.open(new Path("/user/hadoop/text.gz"));
        //把text文件里到数据解压，然后输出到控制台
        InputStream in = codec.createInputStream(inputStream);
        IOUtils.copyBytes(in, System.out, conf);
        IOUtils.closeStream(in);
    }

    /**
     * 使用文件扩展名来推断二来的codec来对文件进行解压缩
     *
     * @param uri
     * @throws IOException
     */
    public static void uncompress1(String uri) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if (codec == null) {
            System.out.println("no codec found for " + uri);
            System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }

    public static void main(String[] args) throws Exception {
        //compress("org.apache.hadoop.io.compress.GzipCodec");
        //uncompress("text");
        uncompress1("/user/text.gz");
    }
}
