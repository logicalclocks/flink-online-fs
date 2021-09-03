package com.logicalclocks.taxiDemo;

import com.fasterxml.jackson.databind.ObjectMapper;
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

public class MapToFeatureGroupRecord extends RichMapFunction<TaxiRide, byte[]> {
  // Primary key name
  String primaryKeyName;

  // Cannot be serialized so we create these in open().
  private transient Schema schema;
  private transient GenericData.Record record;

  // Avro schema in JSON format.
  private final String schemaString;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public MapToFeatureGroupRecord(String primaryKeyName, Schema schema) {
    this.primaryKeyName = primaryKeyName;
    this.schemaString = schema.toString();
    this.record = new GenericData.Record(schema);
  }

  @Override
  public byte[] map(TaxiRide inputClass) throws Exception {
    Map<String, Object> fieldsToValues= objectMapper.convertValue(inputClass, Map.class);

    for (String outputName : fieldsToValues.keySet()) {
      this.record.put(outputName.toLowerCase(), fieldsToValues.get(outputName));
    }
    return encode(this.record);
  }

  @Override
  public void open(Configuration config) {
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
