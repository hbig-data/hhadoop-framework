package com.ryan.hadoop.test;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;

/**
 * @author Rayn on 2017/1/16.
 * @email liuwei412552703@163.com.
 */
public class OtherTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(OtherTestCase.class);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {


    }

    /**
     * @throws Exception
     */
    @Test
    public void testAvro() throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));

        GenericData.Record record = new GenericData.Record(schema);
        record.put("left", new Utf8("L"));
        record.put("right", new Utf8("R"));

        /**
         * 序列化记录到输出流
         */
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        /**
         * 将数据对象翻译成 Encoder 对象可以理解的类型，然后由后者写到输出流
         */
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        /**
         * 写出到输出流
         */
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();

        out.close();

        /**
         * 从字节缓冲区中读回对象
         */
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().createBinaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        System.out.println("Left : " + result.get("left").toString());
        System.out.println("Right : " + result.get("right").toString());
    }

    /**
     * Avro Write to file
     *
     * @throws Exception
     */
    @Test
    public void testAvroWrite2File() throws Exception {

        File file = new File("data.avro");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        GenericData.Record record = new GenericData.Record(schema);
        record.put("left", new Utf8("L"));
        record.put("right", new Utf8("R"));

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(record);

        dataFileWriter.close();
    }

    /**
     * 
     * @throws Exception
     */
    @Test
    public void testAvroReader() throws Exception {
        File file = new File("data.avro");

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, reader);

        System.out.println(dataFileReader.getSchema());

        while(dataFileReader.hasNext()){
            GenericRecord record = dataFileReader.next();
            Object left = record.get("left");
            Object right = record.get("right");

        }

    }
}
