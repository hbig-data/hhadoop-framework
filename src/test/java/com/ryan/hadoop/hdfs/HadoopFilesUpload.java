package com.ryan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

/**
 * Created by Administrator on 2017/1/13.
 */
public class HadoopFilesUpload {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopFilesUpload.class);
    private Configuration configuration = null;
    private FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.newInstance(configuration);
    }

    @After
    public void tearDown() throws Exception {
        if (null != fileSystem) {
            fileSystem.close();
        }
    }

    @Test
    public void testName() throws Exception {
        LOG.info("--------->>>>>>>>>>>Test log output.");
    }

    /**
     * 向Hadoop中写文件
     *
     * @throws Exception
     */
    @Test
    public void testWriteFileToHadoop() throws Exception {
        FSDataOutputStream out = null;
        LOG.info("The test starting........................");
        try {
            String localStr = "D://JF_FTP_RAWLOGUSERBV_003_0001.txt";
// 对应的是本地文件系统的目录
            String dst = "hdfs://nowledgedata-n8:50075/browseDirectory.jsp?dir=%2Ftemp%2Fceshi&namenodeInfoPort=50070&nnaddr=192.168.1.101:9000";
            //创建一个文件系统
            Path srcPath = new Path(localStr);
            Path dstPath = new Path(dst);
            Long start = System.currentTimeMillis();
            fileSystem.copyFromLocalFile(false, srcPath, dstPath);
            System.out.println("Time:" + (System.currentTimeMillis() - start));
            System.out.println("________________________Upload to " + configuration.get("fs.default.name") + "________________________");
            fileSystem.close();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

//            InputStream in = new BufferedInputStream(new FileInputStream(localStr));
//            Configuration conf = new Configuration();
////获得hadoop系统的连接
//            FileSystem fs = FileSystem.get(URI.create(dst),conf);
////out对应的是Hadoop文件系统中的目录
//            out = fs.create(new Path(dst));
//            IOUtils.copyBytes(in, out, 4096,true);//4096是4k字节
//            System.out.println("------------>>>>>>>>>>>>>>>>>>The File have upload success.");
//            LOG.info("The test ending......................");
//        }

    /**
     * 删除 HDFS 文件
     *
     * @throws Exception
     */
    @Test
    public void testHdfsRemoveFile() throws Exception {
        Path lockFile = new Path("/ryan/lucene/");
        fileSystem.delete(lockFile, true);
    }
/**
 * 向Hadoop的hdfs中写入本地文件
 */
    @Test
    public void testAddFile() throws Exception {
        Path sourcePath = new Path("D://testHadoopUpload.txt");
        Path hdfsDst = new Path("/temp/ceshi");
        fileSystem.copyFromLocalFile(sourcePath,hdfsDst);
        System.out.println(">>>>>>>>>>>>>>>>>>>>The file upload have finished");
        fileSystem.close();
    }

}
