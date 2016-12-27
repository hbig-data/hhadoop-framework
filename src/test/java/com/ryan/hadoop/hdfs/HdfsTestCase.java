package com.ryan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/11/22 13:47.
 */
public class HdfsTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTestCase.class);

    private FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
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
        LOG.info("test log output.");

    }

    /**
     * 遍历所有目录
     *
     * @throws Exception
     */
    @Test
    public void testFileStatus() throws Exception {

        LOG.info("初始化完成.");

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatuse : fileStatuses) {
            LOG.info("{} -- {}", fileStatuse.getPath(), fileStatuse.getOwner());

        }

    }

    /**
     * 追加写文件
     * @throws Exception
     */
    @Test
    public void testAppendWriter() throws Exception {

        FSDataOutputStream outputStream = fileSystem.append(new Path("/usr/test"));
        outputStream.write("测试追加写入.".getBytes());

        outputStream.flush();
        outputStream.close();

    }
}
