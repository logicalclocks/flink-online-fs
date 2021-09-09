package com.logicalclocks.aggregations.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HsfsKafkaDeserializer implements KafkaDeserializationSchema<Map<String, Object>> {
  private List<String> sourceFieldNames;
  private String timestampField;
  String eventTimeFormat;
  String eventTimeType;
  private boolean isNestedSchema;
  private final ObjectMapper objectMapper = new ObjectMapper();
  List<String> specFields = Arrays.asList("deserializationTimestamp", "kafkaCommitTimestamp");


  public HsfsKafkaDeserializer (List<String> sourceFieldNames, String timestampField,
                                String eventTimeFormat, String eventTimeType, boolean isNestedSchema) {
    this.sourceFieldNames = sourceFieldNames;
    this.timestampField = timestampField;
    this.eventTimeFormat = eventTimeFormat;
    this.eventTimeType = eventTimeType;
    this.isNestedSchema = isNestedSchema;
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
    if (isNestedSchema) {
      JsonNode jsonNodeRoot = objectMapper.readTree(message);
      for (String fieldName: sourceFieldNames){
        if (!specFields.contains(fieldName)) {
          result.put(fieldName, jsonNodeRoot.findValue(fieldName).textValue());
        }
      }
    } else {
      // convert JSON string to Java Map
      result = objectMapper.readValue(message, Map.class);
    }
    result.put("kafkaCommitTimestamp", timestamp);
    return parseDateToLong(result);
  }

  @Override
  public TypeInformation<Map<String, Object>> getProducedType() {
    TypeInformation<Map<String, Object>> typeInformation = TypeInformation
        .of(new TypeHint<Map<String, Object>>() {
        });
    return typeInformation;
  }

  private Map<String, Object> parseDateToLong (Map<String, Object> element) throws ParseException {
    String datetimeField;
    if (element.containsKey(timestampField)){
      datetimeField = (String) element.get(timestampField);
    } else {
      throw new VerifyError("Provided field " + timestampField + "  doesn't exist");
    }
    Long timeStamp;
    if (eventTimeType.toLowerCase().equals("string")) {
      SimpleDateFormat dateFormat = new SimpleDateFormat(eventTimeFormat);
      timeStamp = dateFormat.parse(datetimeField).getTime();
    } else if (eventTimeType.toLowerCase().equals("long")) {
      timeStamp = Long.valueOf(datetimeField);
    } else {
      throw new VerifyError("For timestampField filed only String and Long types are supported");
    }
    element.put(timestampField, timeStamp);
    element.put("deserializationTimestamp", Instant.now().toEpochMilli());
    return element;
  }
}
