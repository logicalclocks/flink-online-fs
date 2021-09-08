package com.logicalclocks.aggregations.functions;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

public class OutOfOrdernessTimestampAndWatermarksAssigner
    extends BoundedOutOfOrdernessTimestampExtractor<Map<String, Object>>  {

  private String timestampField;

  public OutOfOrdernessTimestampAndWatermarksAssigner(Time maxOutOfOrderness, String timestampField) {
    super(maxOutOfOrderness);
    this.timestampField = timestampField;
  }

  @SneakyThrows
  @Override
  public long extractTimestamp(Map<String, Object> element) {
    return (long) element.get(timestampField);
  }
}
