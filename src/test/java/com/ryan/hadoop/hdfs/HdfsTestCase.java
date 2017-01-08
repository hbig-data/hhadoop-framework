package com.ryan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.solr.request.json.JSONUtil;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/11/22 13:47.
 */
public class HdfsTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTestCase.class);

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
     *
     * @throws Exception
     */
    @Test
    public void testAppendWrite() throws Exception {

        FSDataOutputStream outputStream = null;
        try {
            outputStream = fileSystem.append(new Path("/usr/test"));
            outputStream.write("测试追加写入.".getBytes());

            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            if (null != outputStream) {
                outputStream.close();
            }
        }

    }

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
     *
     * 查看HDFS中元信息
     *
     * @throws Exception
     */
    @Test
    public void testMetadata() throws Exception {
        String fileUri = "/usr/test";

        //实验1:查看HDFS中某文件的元信息
        System.out.println("实验1:查看HDFS中某文件的元信息");
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(fileUri));
        //获取这个文件的基本信息
        if(fileStatus.isDir()==false){
            System.out.println("这是个文件");
        }
        System.out.println("文件路径: "+fileStatus.getPath());
        System.out.println("文件长度: "+fileStatus.getLen());
        System.out.println("文件修改日期： "+new Timestamp(fileStatus.getModificationTime()).toString());
        System.out.println("文件上次访问日期： "+new Timestamp(fileStatus.getAccessTime()).toString());
        System.out.println("文件备份数： "+fileStatus.getReplication());
        System.out.println("文件的块大小： "+fileStatus.getBlockSize());
        System.out.println("文件所有者：  "+fileStatus.getOwner());
        System.out.println("文件所在的分组： "+fileStatus.getGroup());
        System.out.println("文件的 权限： "+fileStatus.getPermission().toString());
        System.out.println();

        //实验2:查看HDFS中某文件的元信息
        String dirUri = "/usr";
        System.out.println("实验2:查看HDFS中某目录的元信息");
        FileStatus dirStatus = fileSystem.getFileStatus(new Path(dirUri));
        //获取这个目录的基本信息
        if(dirStatus.isDir()==true){
            System.out.println("这是个目录");
        }
        System.out.println("目录路径: "+dirStatus.getPath());
        System.out.println("目录长度: "+dirStatus.getLen());
        System.out.println("目录修改日期： "+new Timestamp (dirStatus.getModificationTime()).toString());
        System.out.println("目录上次访问日期： "+new Timestamp(dirStatus.getAccessTime()).toString());
        System.out.println("目录备份数： "+dirStatus.getReplication());
        System.out.println("目录的块大小： "+dirStatus.getBlockSize());
        System.out.println("目录所有者：  "+dirStatus.getOwner());
        System.out.println("目录所在的分组： "+dirStatus.getGroup());
        System.out.println("目录的 权限： "+dirStatus.getPermission().toString());
        System.out.println("这个目录下包含以下文件或目录：");
        for(FileStatus fs : fileSystem.listStatus(new Path(dirUri))){
            System.out.println(fs.getPath());
        }
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testHdfsMetaData() throws Exception {
        String path = "/usr/test";

        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(new Path(path), 14800, 10);

        for (BlockLocation blockLocation : blockLocations) {
            LOG.info("{}", blockLocation.getOffset(), blockLocation.getLength(), Arrays.asList(blockLocation.getTopologyPaths()).toString());

        }


    }

	/**
	*
	*	写入 SequenceFile
	*/
	@Test
    public void testWriteSequenceFile() throws Exception {

        String [] text = new String[]{"first", "two", "thrid", "four", "five"};

        SequenceFile.Writer writer = null;
		try{
			IntWritable key = new IntWritable();
			Text value = new Text();
            Path path = new Path("/usr/test");
            writer = SequenceFile.createWriter(fileSystem, configuration, path, key.getClass(), value.getClass());


//            AbstractFileSystem abstractFileSystem = Hdfs.createFileSystem(path.toUri(), configuration);
//            FileContext context = FileContext.getFileContext(abstractFileSystem, configuration);

            //无压缩
            writer = SequenceFile.createWriter(fileSystem,configuration,path,key.getClass(),value.getClass());
            //记录压缩
            writer = SequenceFile.createWriter(fileSystem,configuration,path,key.getClass(), value.getClass(), SequenceFile.CompressionType.RECORD,new BZip2Codec());

            //块压缩
            writer = SequenceFile.createWriter(fileSystem,configuration,path,key.getClass(), value.getClass(), SequenceFile.CompressionType.BLOCK,new BZip2Codec());

			for(int i = 0; i < 100; i++){
				key.set(100 - i);
				value.set(text[i % text.length]);

				writer.append(key, value);
			}

		} catch(Exception e){
            e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}


	/**
	 * 读取 SequenceFile
	 *
	 */
	@Test
    public void testReadSequenceFile() throws Exception {

		Path path = new Path("/usr/test");

		SequenceFile.Reader reader = null;

		try{
			IntWritable key = new IntWritable();
			Text value = new Text();
			reader = new SequenceFile.Reader(fileSystem, path, configuration);

            //Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            //Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

            while (reader.next(key, value)) {
                System.out.println(key);
                System.out.println(value);
            }

        } catch(Exception e){
            e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
