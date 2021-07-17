package com.logicalclocks.flink.hsfs.synk;

import lombok.SneakyThrows;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// https://github.com/pravega/flink-tools/blob/8efdbfec830b9b58c01c3bd63c6bd86b94d1c3a5/flink-tools/src/main/java/io/pravega/flinktools/util/JsonToGenericRecordMapFunction.java
// https://github.com/pravega/flink-tools/blob/8efdbfec830b9b58c01c3bd63c6bd86b94d1c3a5/flink-tools/src/main/java/io/pravega/flinktools/StreamToCsvFileJob.java

public class GenericRecordMapFunction extends RichMapFunction<Tuple4<Long, Long, Double, Double>, byte[]> {

  private List<String> fields;

  // Avro schema in JSON format.
  private final String schemaString;

  // Cannot be serialized so we create these in open().
  private transient Schema schema;
  private transient GenericData.Record record;

  public GenericRecordMapFunction(Schema schema, List<String> fields) {
    this.schemaString = schema.toString();
    this.fields = fields;
  }

  @Override
  public void open(Configuration parameters) {
    this.schema = new Schema.Parser().parse(this.schemaString);
    this.record = new GenericData.Record(this.schema);
  }

  @SneakyThrows
  @Override
  public byte[] map(Tuple4<Long, Long, Double, Double> aggregations) {
    record.put(fields.get(0), aggregations.getField(0));
    record.put(fields.get(1), aggregations.getField(1));
    record.put(fields.get(2), aggregations.getField(2));
    record.put(fields.get(3), aggregations.getField(3));
    return encode(record);
  }

  private byte[] encode(GenericRecord record) throws IOException {
    List<GenericRecord> records = new ArrayList<>();
    records.add(record);

    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();
    BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
    for(GenericRecord segment: records) {
      datumWriter.write(segment, binaryEncoder);
    }
    binaryEncoder.flush();
    byte[] bytes = byteArrayOutputStream.toByteArray();
    return bytes;
  }
}