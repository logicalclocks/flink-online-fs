package io.hops.examples.flink.hsfs;

import lombok.SneakyThrows;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class AvroKafkaSink implements KafkaSerializationSchema<byte[]> {
  private String keyField;
  private String topic;

  public AvroKafkaSink(String topic) {
    this.topic = topic;
  }

  public AvroKafkaSink(String keyField, String topic) {
    this.keyField = keyField;
    this.topic = topic;
  }

  @SneakyThrows
  @Override
  public ProducerRecord<byte[], byte[]> serialize(byte[] value, @Nullable Long timestamp) {
    byte[] key = this.keyField.getBytes();
    return new ProducerRecord(this.topic, key, value);
  }
}
