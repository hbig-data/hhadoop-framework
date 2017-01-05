package com.ryan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/5 14:22.
 */
public class HdfsLuceneTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsLuceneTestCase.class);


    /**
     * 创建 Lucene 索引到 HDFS
     *
     * @throws Exception
     */
    @Test
    public void testLuceneIndex2HDFS() throws Exception {
        boolean create = true;

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        Path indexPath = new Path("/ryan/lucene/");
        if (!fs.exists(indexPath)) {
            fs.mkdirs(indexPath);
        }

        /**
         * 创建索引
         */
//        Directory fsDirectory = FSDirectory.open(Paths.get(indexPath.toString()));
        HdfsDirectory hdfsDirectory = new HdfsDirectory(indexPath, conf);
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

        if (create) {
            // Create a new index in the directory, removing any previously indexed documents:
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        } else {
            // Add new documents to an existing index:
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        }

        // Optional: for better indexing performance, if you are indexing many documents, increase the RAM
        // buffer.  But if you do this, increase the max heap size to the JVM (eg add -Xmx512m or -Xmx1g):
        //
        // iwc.setRAMBufferSizeMB(256.0);
        IndexWriter writer = new IndexWriter(hdfsDirectory, iwc);

        Document doc = new Document();
        doc.add(new LongPoint("ids", 11));
        doc.add(new StringField("content", "测试创建一个索引数据", Field.Store.YES));

        writer.addDocument(doc);
        writer.commit();
        //如果是创建和查询在一起执行，不能先关闭，否则会造成查询失败的错误。
        //writer.close();

        /**
         * 获取 HDFS 索引数据，查询
         */
        IndexReader reader = DirectoryReader.open(hdfsDirectory);
        System.out.println(reader.numDocs());
        for (int i = 0; i < reader.numDocs(); i++) {
            Document document = reader.document(i);
            LOG.info("查询结果:{} -- {}", document.getValues("ids"), document.get("content"));
        }

        reader.close();
    }

    @Test
    public void testLuceneSearch() throws Exception {
        Configuration conf = new Configuration();

        Path indexPath = new Path("/ryan/lucene/");
        /**
         * 获取 HDFS 索引数据，查询
         */
        HdfsDirectory hdfsDirectory = new HdfsDirectory(indexPath, conf);
        IndexReader reader = DirectoryReader.open(hdfsDirectory);
        System.out.println(reader.numDocs());
        for (int i = 0; i < reader.numDocs(); i++) {
            Document document = reader.document(i);
            List<IndexableField> fields = document.getFields();
            for (IndexableField field : fields) {
                LOG.info("查询结果:{}", field.stringValue());
            }
        }

        reader.close();

    }
}
