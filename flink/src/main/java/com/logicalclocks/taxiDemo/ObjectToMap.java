package com.logicalclocks.taxiDemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

public class ObjectToMap implements MapFunction {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Map<String, Object> map(Object inputClass) throws Exception {
    return objectMapper.convertValue(inputClass, Map.class);
  }
}
