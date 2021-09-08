package com.logicalclocks.aggregations.functions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AggregateRichWindowFunction extends RichWindowFunction<Map<String, Object>, byte[], Object, TimeWindow> {

  // Primary key name
  String primaryKeyName;

  // field to aggregation method
  Map<String, Map<String, String>> fieldsToAggregation;

  // whether to output window end or not
  private boolean windowStart;
  private boolean windowEnd;
  private boolean aggregationStartTime;
  private boolean aggregationEndTime;


  // descriptive statistics
  private DescriptiveStatistics descriptiveStatistics;

  // TODO (davit): why not schema directly?
  // Avro schema in JSON format.
  private final String schemaString;

  // Cannot be serialized so we create these in open().
  private transient Schema schema;
  private transient GenericData.Record record;

  List<String> specMethods = Arrays.asList("count", "max_processing_delay", "max_event_timestamp");

  public AggregateRichWindowFunction(String primaryKeyName, Schema schema, Map<String, Map<String, String>>
      fieldsToAggregation, boolean windowStart, boolean windowEnd, boolean aggregationStartTime,
                                     boolean aggregationEndTime) {
    this.primaryKeyName = primaryKeyName;
    // TODO (davit): why not schema directly?
    this.schemaString = schema.toString();
    this.fieldsToAggregation = fieldsToAggregation;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.aggregationStartTime = aggregationStartTime;
    this.aggregationEndTime = aggregationEndTime;
  }

  @Override
  public void apply(Object key, TimeWindow timeWindow, Iterable<Map<String, Object>> iterable,
                    Collector<byte[]> collector) throws Exception {
    // start aggregations
    if (aggregationStartTime){
      record.put("aggregation_start_time", new Date().getTime());
    }
    for (String outputName : fieldsToAggregation.keySet()) {
      Map<String, String> aggregationToFeature = fieldsToAggregation.get(outputName);
      for (String field : aggregationToFeature.keySet()) {
        Object aggValue = windowAggregationStats(field, aggregationToFeature.get(field), iterable);
        record.put(outputName, aggValue);
      }
    }
    record.put(primaryKeyName, key);
    if (windowStart) {
      record.put("start",  timeWindow.getEnd());
    }
    if (windowEnd) {
      record.put("end",  timeWindow.getStart());
    }
    if (aggregationEndTime){
      record.put("aggregation_end_time",  new Date().getTime());
    }
    collector.collect(encode(record));
  }

  @Override
  public void open(Configuration parameters) {
    this.descriptiveStatistics = new DescriptiveStatistics();
    this.schema = new Schema.Parser().parse(this.schemaString);
    this.record = new GenericData.Record(this.schema);
  }

  private byte[] encode(GenericRecord record) throws IOException {
    List<GenericRecord> records = new ArrayList<>();
    records.add(record);

    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.reset();
    BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
    for(GenericRecord segment: records) {
      datumWriter.write(segment, binaryEncoder);
    }
    binaryEncoder.flush();
    byte[] bytes = byteArrayOutputStream.toByteArray();
    return bytes;
  }

  private Long processingDelay(Long eventTimeStamp, Long aggregationStartTime) {
    return aggregationStartTime - eventTimeStamp;
  }

  private Object windowAggregationStats(String field, String method, Iterable<Map<String, Object>> iterable) {

    long count = 0;
    for (Map<String, Object> data: iterable) {
      count++;
      if (!specMethods.contains(method)){
        descriptiveStatistics.addValue((double) data.get(field));
      }
      if (method.equals("max_processing_delay")) {
        descriptiveStatistics.addValue((double) processingDelay((long) data.get(field), new Date().getTime()));
      } else if (method.equals("max_event_timestamp")) {
        descriptiveStatistics.addValue(Double.valueOf((long) data.get(field)));
      }
    }

    switch(method) {
      case "average":
        // average
        return descriptiveStatistics.getSum() / count;
      case "min":
        // min
        return descriptiveStatistics.getMin();
      case "max":
        // max
        return descriptiveStatistics.getMax();
      case "max_processing_delay":
        // max
        return Math.round(descriptiveStatistics.getMax());
      case "max_event_timestamp":
        // max
        return Math.round(descriptiveStatistics.getMax());
      case "sum":
        // sum
        return descriptiveStatistics.getSum();
      case "sumsq":
        // sum of the squares
        return descriptiveStatistics.getSumsq();
      case "stdev":
        // standard deviation
        return descriptiveStatistics.getStandardDeviation();
      case "variance":
        // variance
        return descriptiveStatistics.getVariance();
      case "geometric_mean":
        // geometric mean
        return descriptiveStatistics.getGeometricMean();
      case "skewness":
        // skewness of the available values
        return descriptiveStatistics.getSkewness();
      case "kurtosis":
        // Kurtosis of the available values
        return descriptiveStatistics.getKurtosis();
      case "count":
        // count
        return count;
      default:
        return null;
    }
  }
}
