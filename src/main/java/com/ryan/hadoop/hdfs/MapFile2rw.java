package com.ryan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * @author Rayn on 2017/1/16.
 * @email liuwei412552703@163.com.
 */
public class MapFile2rw {

    private Path path = null;
    private Configuration configuration = null;

    private String[] datas = new String[]{
            "this is first data",
            "I have an apple",
            "you have a pen"
    };

    public MapFile2rw() {
        this.configuration = new Configuration();
    }

    /**
     * MapFile Writer
     */
    public void mapFileWriter() throws IOException {

        MapFile.Writer writer = new MapFile.Writer(configuration, path);

        IntWritable key = new IntWritable();
        Text value = new Text();

        try {
            for (int i = 0; i < 100; i++) {
                key.set(i);
                value.set(datas[i % datas.length]);

                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }


    }

    /**
     * Map File Reader
     */
    public void mapFileReader() throws IOException {

        IntWritable key = new IntWritable();
        Text value = new Text();

        MapFile.Reader reader = new MapFile.Reader(path, configuration);

        while (reader.next(key, value)) {
            System.out.println("Key :" + key + ", Value : " + value);
        }

        IOUtils.closeStream(reader);
    }

    /**
     *
     */
    public void setFileWrite() throws IOException {

        MapFile.Merger merger = new MapFile.Merger(configuration);

        Path[] paths = {new Path(""), new Path("")};

        merger.merge(paths, false, new Path("/temp/meger/out"));


    }

    /**
     *
     * @throws IOException
     */
    public void mapFileFixer() throws Exception {
        FileSystem system = FileSystem.get(configuration);

        Path inPath = new Path("");
        SequenceFile.Reader reader = new SequenceFile.Reader(system, inPath, configuration);

        Class keyClass = reader.getKeyClass();
        Class valueClass = reader.getValueClass();

        /**
         * fix 通常用于重建已损坏的索引，但是由于它能从头开始建立新的索引
         */
        long entries = MapFile.fix(system, inPath, keyClass, valueClass, false, configuration);

        System.err.printf("Created MapFile %s with %d entries\n", inPath, entries);




    }
}
