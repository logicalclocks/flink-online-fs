package com.logicalclocks.flink.hsfs.functions;

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
import java.util.List;
import java.util.Map;

public class AggregateRichWindowFunction extends RichWindowFunction<Map<String, Object>, byte[], Object, TimeWindow> {

  // Primary key name
  String primaryKeyName;

  // field to aggregation method
  Map<String, Map<String, String>> fieldsToAggregation;

  // descriptive statistics
  private DescriptiveStatistics descriptiveStatistics;

  // TODO (davit): why not schema directly?
  // Avro schema in JSON format.
  private final String schemaString;

  // Cannot be serialized so we create these in open().
  private transient Schema schema;
  private transient GenericData.Record record;

  public AggregateRichWindowFunction(String primaryKeyName, Schema schema, Map<String, Map<String, String>>
      fieldsToAggregation) {
    this.primaryKeyName = primaryKeyName;
    // TODO (davit): why not schema directly?
    this.schemaString = schema.toString();
    this.fieldsToAggregation = fieldsToAggregation;
  }

  @Override
  public void apply(Object key, TimeWindow timeWindow, Iterable<Map<String, Object>> iterable,
                    Collector<byte[]> collector) throws Exception {
    for (String outputName : fieldsToAggregation.keySet()) {
      Map<String, String> aggregationToFeature = fieldsToAggregation.get(outputName);
      for (String field : aggregationToFeature.keySet()) {
        Object aggValue = windowAggregationStats(field, aggregationToFeature.get(field), iterable);
        record.put(outputName, aggValue);
      }
    }
    record.put(primaryKeyName, key);
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

  private Object windowAggregationStats(String field, String method, Iterable<Map<String, Object>> iterable) {

    long count = 0;
    for (Map<String, Object> data: iterable) {
      count++;
      if (!method.equals("count")){
        descriptiveStatistics.addValue((double) data.get(field));
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
