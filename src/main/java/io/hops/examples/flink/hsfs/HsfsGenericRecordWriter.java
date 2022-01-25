package io.hops.examples.flink.hsfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HsfsGenericRecordWriter extends RichMapFunction <Map<String, Object>, byte[]> {

  // Avro schema in JSON format.
  private String schemaString;

  // Cannot be serialized so we create these in open().
  private transient Schema schema;
  private transient GenericData.Record record;

  public HsfsGenericRecordWriter(Schema schema){
    this.schemaString = schema.toString();
  }

  @Override
  public byte[] map(Map<String, Object> agg) throws Exception {
    agg.forEach((k,v) -> record.put(k,v));
    return encode(record);
  }

  @Override
  public void open(Configuration parameters) {
    this.schema = new Schema.Parser().parse(this.schemaString);
    this.record = new GenericData.Record(this.schema);
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
