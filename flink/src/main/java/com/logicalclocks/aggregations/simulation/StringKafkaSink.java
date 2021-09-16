package com.logicalclocks.aggregations.simulation;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class StringKafkaSink implements KafkaSerializationSchema<String> {
  private String topic;

  public StringKafkaSink(String topic) {
    this.topic = topic;
  }


  @Override
  public ProducerRecord<byte[], byte[]> serialize(String value, @Nullable Long timestamp) {
    return new ProducerRecord(this.topic, value.getBytes(StandardCharsets.UTF_8));
  }
}
