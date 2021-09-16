package com.logicalclocks.aggregations.simulation;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import lombok.SneakyThrows;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;

public class AvroKafkaSync implements KafkaSerializationSchema<StoreEvent> {

  private String topic;

  public AvroKafkaSync(String topic) {
    this.topic = topic;
  }

  @SneakyThrows
  @Override
  public ProducerRecord<byte[], byte[]> serialize(StoreEvent storeEvent, @Nullable Long aLong) {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<StoreEvent> dataFileWriter = new SpecificDatumWriter<StoreEvent>(storeEvent.getSchema());
    dataFileWriter.write(storeEvent, encoder);
    encoder.flush();

    return new ProducerRecord(this.topic, out.toByteArray());
  }
}
