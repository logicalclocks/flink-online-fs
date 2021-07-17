package com.logicalclocks.flink.hsfs.synk;

import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroKafkaSink implements KafkaSerializationSchema<GenericRecord> {
  private final String keyField;
  private final String topic;
  private String userSchema;

  public AvroKafkaSink(String userSchema, String keyField, String topic) {
    this.keyField = keyField;
    this.topic = topic;
    this.userSchema = userSchema;
  }

  @SneakyThrows
  @Override
  public ProducerRecord<byte[], byte[]> serialize(GenericRecord element, @Nullable Long timestamp) {
    List<GenericRecord> records = new ArrayList<>();
    records.add(element);

    final Schema.Parser parser = new Schema.Parser();
    final Schema schema = parser.parse(userSchema);

    byte[] value = encode(records, schema);
    byte[] key = this.keyField.getBytes();
    return new ProducerRecord(this.topic, key, value);
  }

  private byte[] encode(List<GenericRecord> records, Schema schema) throws IOException {
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