package com.logicalclocks.flink.hsfs.synk;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class AvroKafkaSink implements KafkaSerializationSchema<GenericRecord> {
  private String keyField;

  public AvroKafkaSink(String keyField) {
    this.keyField = keyField;
  }
  @Override
  public ProducerRecord<byte[], byte[]> serialize(GenericRecord element, @Nullable Long timestamp) {
    byte[] value = element.toString().getBytes();
    String key = String.valueOf(element.get(this.keyField));
    return new ProducerRecord<byte[], byte[]>(key, value);
  }
}