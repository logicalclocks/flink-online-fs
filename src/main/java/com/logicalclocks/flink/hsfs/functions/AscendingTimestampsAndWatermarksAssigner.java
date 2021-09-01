package com.logicalclocks.flink.hsfs.functions;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class AscendingTimestampsAndWatermarksAssigner extends AscendingTimestampExtractor<Map<String, Object>> {

  private String timestampField;
  private String eventTimeFormat;
  private String eventTimeType;

  public AscendingTimestampsAndWatermarksAssigner(String timestampField, String eventTimeFormat, String eventTimeType) {
    this.timestampField = timestampField;
    this.eventTimeFormat = eventTimeFormat;
    this.eventTimeType = eventTimeType;
  }

  @Override
  public long extractAscendingTimestamp(Map<String, Object> element) {
    String datetimeField;
    if (element.containsKey(timestampField)){
      datetimeField = (String) element.get(timestampField);
    } else {
      throw new VerifyError("Provided field doesn't exist");
    }
    Long timeStamp = null;
    if (eventTimeType.toLowerCase().equals("string")) {
      SimpleDateFormat dateFormat = new SimpleDateFormat(eventTimeFormat);
      try {
        timeStamp = dateFormat.parse(datetimeField).getTime();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    } else if (eventTimeType.toLowerCase().equals("long")) {
      timeStamp = Long.valueOf(timestampField);
    } else {
      throw new VerifyError("For timestampField filed only String and Long types are supported");
    }
    return timeStamp;
  }
}
