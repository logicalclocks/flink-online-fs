package com.logicalclocks.aggregations.functions;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.logicalclocks.aggregations.avroSchemas.*;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class MyAvroDeserializer implements KafkaDeserializationSchema<Map<String, Object>> {
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
  private String timestampField;
  String eventTimeFormat;
  String eventTimeType;


  public MyAvroDeserializer(String timestampField, String eventTimeFormat, String eventTimeType) {
    this.timestampField = timestampField;
    this.eventTimeFormat = eventTimeFormat;
    this.eventTimeType = eventTimeType;
  };

  @Override
  public boolean isEndOfStream(Map<String, Object> stringObjectMap) {
    return false;
  }

  @Override
  public Map<String, Object> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

    byte[] messageKey = consumerRecord.key();
    byte[] message = consumerRecord.value();
    long offset = consumerRecord.offset();
    long timestamp = consumerRecord.timestamp();

    Map<String, Object> result= new HashMap<>();

    SpecificDatumReader<StoreEvent> reader = new SpecificDatumReader<>(StoreEvent.class);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message,null);
    StoreEvent deserializedEvent = reader.read(null,decoder);

    /*
    DeserializationSchema<StoreEvent> deserializer =
        AvroDeserializationSchema.forSpecific(StoreEvent.class);
    StoreEvent deserializedEvent = deserializer.deserialize(message);
     */

    result.put("customer_id", deserializedEvent.getEventDefinitions().getContexts().getUserContext().getCustomerId().toString());
    result.put("event_type", deserializedEvent.getEventType().toString());
    result.put(timestampField, dateFormat.parse(deserializedEvent.getReceivedTs().toString()).getTime());
    result.put("deserializationTimestamp", Instant.now().toEpochMilli());
    result.put("kafkaCommitTimestamp", timestamp);
    return result;
  }

  @Override
  public TypeInformation<Map<String, Object>> getProducedType() {
    TypeInformation<Map<String, Object>> typeInformation = TypeInformation
        .of(new TypeHint<Map<String, Object>>() {
        });
    return typeInformation;
  }

}
