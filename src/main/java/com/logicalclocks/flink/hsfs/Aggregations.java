package com.logicalclocks.flink.hsfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.flink.hsfs.functions.AggregateRichWindowFunction;
import com.logicalclocks.flink.hsfs.synk.AvroKafkaSink;
import com.logicalclocks.flink.hsfs.utils.Utils;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Aggregations {

  // https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/operators/windows/

  // https://github.com/andresionek91/fake-web-events/blob/master/examples/print_events.py
  // https://github.com/apache/flink-training/blob/master/common/src/main/java/org/apache/flink/training/exercises/common/sources/TaxiRideGenerator.java
  // http://vishnuviswanath.com/flink_eventtime.html
  // https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/
  // https://programmer.group/detailed-explanation-of-watermark-in-flink-eventtime.html

  private Utils utils = new Utils();

  public void run() throws Exception {

    // define stream env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // the stream holding the file content
    Map<Object, Object> aggregationSpecs;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("json/fg_job_config.json");
    try {
      aggregationSpecs =
          new ObjectMapper().readValue(inputStream, HashMap.class);
      inputStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    String source_topic = (String) aggregationSpecs.get("online_source");
    String timestampField = (String) aggregationSpecs.get("event_time");
    String eventTimeFormat = (String) aggregationSpecs.get("event_time_format");
    String eventTimeType = (String) aggregationSpecs.get("event_time_type");

    // get source stream
    DataStream<Map<String, Object>> sourceStream = utils.getSourceKafkaStream(env, source_topic,
        timestampField, eventTimeFormat, eventTimeType);

    /*
    String kafkaConfig = null;
    if (kafkaConfig != null) {
      Map<String, String> kafkaSpecs =
          new ObjectMapper().readValue(kafkaConfig, HashMap.class);
      sourceStream = utils.getSourceKafkaStream(env, kafkaSpecs, source_topic,
          timestampField, dateTimeFormat);
    } else {
      sourceStream = utils.getSourceKafkaStream(env, source_topic,
          timestampField, dateTimeFormat);
    }
    */

    // get hsfs handle
    FeatureStore fs = utils.getFeatureStoreHandle();

    // get feature groups
    FeatureGroup featureGroup = fs.getFeatureGroup((String) aggregationSpecs.get("feature_group_name"),
        (Integer) aggregationSpecs.get("feature_group_version"));

    // compute aggregations
    Map<String, Map<String, String>> aggregations = (Map<String, Map<String, String>>)
        aggregationSpecs.get("aggregations");

    List<String> primaryKeys = featureGroup.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    DataStream<byte[]> aggregationStream =
        sourceStream.keyBy(r -> r.get(primaryKeys.get(0)))
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .apply(new AggregateRichWindowFunction(primaryKeys.get(0), featureGroup.getDeserializedAvroSchema(),
                aggregations));

    //send to online fg topic
    Properties featureGroupKafkaPropertiies = utils.getKafkaProperties(featureGroup);
    aggregationStream.addSink(new FlinkKafkaProducer<byte[]>(featureGroup.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", primaryKeys), featureGroup.getOnlineTopicName()),
        featureGroupKafkaPropertiies,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    Aggregations aggregations = new Aggregations();
    aggregations.run();
  }
}
