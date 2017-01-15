package com.ryan.hadoop.test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * 压缩解压缩算法
 *
 * @author Rayn on 2017/1/15.
 * @email liuwei412552703@163.com.
 */
public class CodecTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(CodecTestCase.class);

    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        //List<Class<? extends CompressionCodec>> codecClasses = CompressionCodecFactory.getCodecClasses(new Configuration());

        configuration = new Configuration();

    }

    /**
     * @throws Exception
     */
    @Test
    public void testDefault() throws Exception {

        DefaultCodec codec = new DefaultCodec();

        CompressionOutputStream codecOutputStream = codec.createOutputStream(System.out);

        IOUtils.copy(System.in, codecOutputStream);

        codecOutputStream.finish();
        codecOutputStream.close();

    }

    /**
     * @throws Exception
     */
    @Test
    public void testGzip() throws Exception {
        GzipCodec codec = new GzipCodec();

        CompressionOutputStream codecOutputStream = codec.createOutputStream(System.out);

        IOUtils.copy(System.in, codecOutputStream);

        codecOutputStream.finish();
        codecOutputStream.close();
    }

    /**
     * @throws Exception
     */
    @Test
    public void testBzip2() throws Exception {

        BZip2Codec bZip2Codec = new BZip2Codec();

    }

    /**
     * Lz4
     *
     * @throws Exception
     */
    @Test
    public void testLz4() throws Exception {
        Lz4Codec lz4Codec = new Lz4Codec();

    }

    /**
     * @throws Exception
     */
    @Test
    public void testSnappy() throws Exception {
        SnappyCodec snappyCodec = new SnappyCodec();

    }

    /**
     * LZO
     *
     * @throws Exception
     */
    @Test
    public void testLzo() throws Exception {
        LzopCodec codec = new LzopCodec();
    }

    /**
     * 通过 CompressionCodecFactory 获取压缩类型
     *
     * @throws Exception
     */
    @Test
    public void testCompressionCodecFactory() throws Exception {
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());

        Path path = new Path("/temp/test.gz");
        CompressionCodec codec = codecFactory.getCodec(path);

        if (null == codec) {
            System.err.println("No Codec Found for " + path.toUri());
            return;
        }


        String outputUri = CompressionCodecFactory.removeSuffix(path.toUri().toString(), codec.getDefaultExtension());

        // 进行压缩执行逻辑


    }

    /**
     * 在执行大量的 压缩和解压缩中，可以考虑使用 CodecPool，它支持反复的使用压缩和解压缩，以分摊这些对象的开销
     * @throws Exception
     */
    @Test
    public void testCodecPool() throws Exception {

        String codecClassName = "";
        Class<?> aClass = Class.forName(codecClassName);
        CompressionCodec compressionCodec =(CompressionCodec) ReflectionUtils.newInstance(aClass, configuration);

        Compressor compressor = CodecPool.getCompressor(compressionCodec, configuration);

        try {
            CompressionOutputStream outputStream = compressionCodec.createOutputStream(System.out, compressor);

            IOUtils.copy(System.in, outputStream);
            outputStream.finish();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            CodecPool.returnCompressor(compressor);
        }

    }
}
