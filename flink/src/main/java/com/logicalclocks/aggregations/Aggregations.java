package com.logicalclocks.aggregations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.aggregations.functions.AggregateRichWindowFunction;
import com.logicalclocks.aggregations.synk.AvroKafkaSink;
import com.logicalclocks.aggregations.utils.Utils;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Aggregations {

  private Utils utils = new Utils();

  public void run() throws Exception {

    // define stream env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // aggregations config
    // the stream holding the file content
    Map<Object, Object> aggregationSpecs;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("json/flink_aggregations_config.json");
    try {
      aggregationSpecs =
          new ObjectMapper().readValue(inputStream, HashMap.class);
      inputStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    String keyName = (String) aggregationSpecs.get("key");
    Map<Object, Object> onlineSource = (Map<Object, Object>) aggregationSpecs.get("online_source");
    String source_topic = (String) onlineSource.get("topic_name");
    boolean externalKafka = (boolean) onlineSource.get("external_kafka");

    String timestampField = (String) aggregationSpecs.get("event_time");
    String eventTimeFormat = (String) aggregationSpecs.get("event_time_format");
    String eventTimeType = (String) aggregationSpecs.get("event_time_type");
    Integer windowSize = (Integer) aggregationSpecs.get("window_size");
    String windowTimeUnit = (String) aggregationSpecs.get("window_time_unit");
    Integer watermark = (Integer) aggregationSpecs.get("watermark");
    String watermarkTimeUnit = (String) aggregationSpecs.get("watermark_time_unit");
    String windowType = (String) aggregationSpecs.get("window_type");
    Integer slideSize = (Integer) aggregationSpecs.get("slide_size");
    String slideTimeUnit = (String) aggregationSpecs.get("slide_time_unit");
    Integer gapSize = (Integer) aggregationSpecs.get("gap_size");
    String gapTimeUnit = (String) aggregationSpecs.get("gap_time_unit");
    boolean windowStart = (boolean) aggregationSpecs.get("window_start");
    boolean windowEnd  = (boolean) aggregationSpecs.get("window_end");

    Map<String, Object> sourceSilters = (Map<String, Object>) aggregationSpecs.get("source_filters");
    // get source stream
    DataStream<Map<String, Object>> sourceStream;
    if(!externalKafka){
      sourceStream = utils.getSourceKafkaStream(env, source_topic,
          timestampField, eventTimeFormat, eventTimeType, watermark, watermarkTimeUnit);
    } else {
      Map<String, String> externalKafkaConfig = (Map<String, String>) onlineSource.get("external_kafka_config");
      sourceStream = utils.getSourceKafkaStream(env, externalKafkaConfig, source_topic,
          timestampField, eventTimeFormat, eventTimeType, watermark, watermarkTimeUnit);
    }

    // get hsfs handle
    FeatureStore fs = utils.getFeatureStoreHandle();

    // get feature groups
    FeatureGroup featureGroup = fs.getFeatureGroup((String) aggregationSpecs.get("feature_group_name"),
        (Integer) aggregationSpecs.get("feature_group_version"));

    Map<String, Map<String, String>> aggregations = (Map<String, Map<String, String>>)
        aggregationSpecs.get("aggregations");
    List<String> primaryKeys = featureGroup.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());

    // filter if any
    if (sourceSilters != null || !sourceSilters.isEmpty()){
      for (String filterKey: sourceSilters.keySet()){
        sourceStream = sourceStream.keyBy(r -> r.get(keyName)).
            filter(r -> r.get(filterKey).equals(sourceSilters.get(filterKey)));
      }
    }

    // compute aggregations
    DataStream<byte[]> aggregationStream =
        sourceStream.keyBy(r -> r.get(keyName))
            .window(utils.inferWindowType(windowType, utils.inferTimeSize(windowSize, windowTimeUnit),
                utils.inferTimeSize(slideSize, slideTimeUnit), utils.inferTimeSize(gapSize, gapTimeUnit)))
            .apply(new AggregateRichWindowFunction(primaryKeys.get(0), featureGroup.getDeserializedAvroSchema(),
                aggregations, windowStart, windowEnd));

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
