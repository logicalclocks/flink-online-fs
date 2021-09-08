package com.logicalclocks.aggregations.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringToMapDeserializationSchema implements DeserializationSchema<Map<String, Object>> {

  private List<String> sourceFieldNames;
  private String timestampField;
  String eventTimeFormat;
  String eventTimeType;
  private boolean isNestedSchema;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public StringToMapDeserializationSchema(List<String> sourceFieldNames, String timestampField,
                                          String eventTimeFormat, String eventTimeType, boolean isNestedSchema) {
    this.sourceFieldNames = sourceFieldNames;
    this.timestampField = timestampField;
    this.eventTimeFormat = eventTimeFormat;
    this.eventTimeType = eventTimeType;
    this.isNestedSchema = isNestedSchema;
  }

  @SneakyThrows
  @Override
  public Map<String, Object> deserialize(byte[] message) throws IOException {

    Map<String, Object> result= new HashMap<>();
    if (isNestedSchema) {
      JsonNode jsonNodeRoot = objectMapper.readTree(message);
      for (String fieldName: sourceFieldNames){
        result.put(fieldName, jsonNodeRoot.findValue(fieldName).textValue());
      }
    } else {
      // convert JSON string to Java Map
      result = objectMapper.readValue(message, Map.class);
    }
    return parseDateToLong(result);
  }

  @Override
  public boolean isEndOfStream(Map<String, Object> stringObjectMap) {
    return false;
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
    return element;
  }
}
