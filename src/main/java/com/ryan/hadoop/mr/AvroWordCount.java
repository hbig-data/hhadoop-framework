package com.ryan.hadoop.mr;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.*;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

/**
 * @author Rayn on 2017/1/16.
 * @email liuwei412552703@163.com.
 */
public class AvroWordCount extends Configured implements Tool {
    private Schema SCHEMA;

    {
        try {
            SCHEMA = new Schema.Parser().parse(new File(""));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class AvrpMap extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {

        private GenericRecord record = new GenericData.Record(SCHEMA);

        @Override
        public void map(Utf8 datum, AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter) throws IOException {
            record.put("key", "value");

            collector.collect(new Pair<Integer, GenericRecord>(SCHEMA, record));
        }
    }

    private class AvroReducer extends org.apache.avro.mapred.AvroReducer<Integer, GenericRecord, GenericRecord> {

        @Override
        public void reduce(Integer key, Iterable<GenericRecord> values, AvroCollector<GenericRecord> collector, Reporter reporter) throws IOException {


        }
    }


    @Override
    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), getClass());
        jobConf.setJobName("avroWordCount");

        FileInputFormat.addInputPath(jobConf, new Path("/temp/test.txt"));
        FileOutputFormat.setOutputPath(jobConf, new Path("/temp/out"));

        AvroJob.setInputSchema(jobConf, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputSchema(jobConf, Pair.getPairSchema(Schema.create(Schema.Type.INT), SCHEMA));
        AvroJob.setOutputSchema(jobConf, SCHEMA);

        jobConf.setInputFormat(AvroUtf8InputFormat.class);

        AvroJob.setMapperClass(jobConf, AvroMapper.class);
        AvroJob.setReducerClass(jobConf, AvroReducer.class);


        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new AvroWordCount(), args);

        System.exit(status);

    }
}
