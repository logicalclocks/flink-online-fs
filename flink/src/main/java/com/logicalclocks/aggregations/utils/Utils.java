package com.logicalclocks.aggregations.utils;

import com.logicalclocks.aggregations.functions.OutOfOrdernessTimestampAndWatermarksAssigner;
import com.logicalclocks.aggregations.functions.StringToMapDeserializationSchema;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.metadata.KafkaApi;

import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Utils {

  private KafkaApi kafkaApi = new KafkaApi();

  public Properties getKafkaProperties() throws Exception {
    Properties dataKafkaProps = new Properties();
    String materialPasswd = readMaterialPassword();
    dataKafkaProps.setProperty("bootstrap.servers", "broker.kafka.service.consul:9091");
    // These settings are static and they don't need to be changed
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");

    return dataKafkaProps;
  }

  public Properties getKafkaProperties(FeatureGroup featureGroup) throws Exception {
    Properties dataKafkaProps = new Properties();
    String materialPasswd = readMaterialPassword();
    dataKafkaProps.setProperty("bootstrap.servers",
        kafkaApi.getBrokerEndpoints(featureGroup.getFeatureStore()).stream().map(broker -> broker.replaceAll(
            "INTERNAL://", "")).collect(Collectors.joining(",")));
    // These settings are static and they don't need to be changed
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");

    return dataKafkaProps;
  }

  public Properties getKafkaProperties(Map<String, String> propsMap) throws Exception {
    Properties dataKafkaProps = new Properties();
    for (String key: propsMap.keySet()) {
      dataKafkaProps.setProperty(key, propsMap.get(key));
    }

    return dataKafkaProps;
  }

  /**
   * Setup the Kafka source stream.
   *
   * The Kafka topic is populated by the same producer notebook.
   * The stream at this stage contains just string.
   *
   * @param env The Stream execution environment to which add the source
   * @param sourceTopic the Kafka topic to read the data from
   * @param timestampField
   * @param eventTimeFormat
   * @param eventTimeType
   * @return the DataStream object
   * @throws Exception
   */

  public DataStream<Map<String, Object>> getSourceKafkaStream(StreamExecutionEnvironment env,
                                                              String sourceTopic,
                                                              String timestampField,
                                                              String eventTimeFormat,
                                                              String eventTimeType,
                                                              Integer watermarkSize,
                                                              String watermarkTimeUnit,
                                                              List<String> sourceFieldNames,
                                                              boolean isNestedSchema)
      throws Exception {

    Properties kafkaProperties = getKafkaProperties();
    FlinkKafkaConsumerBase<Map<String, Object>> kafkaSource = new FlinkKafkaConsumer<>(
        sourceTopic, new StringToMapDeserializationSchema(sourceFieldNames, timestampField, eventTimeFormat,
        eventTimeType,  isNestedSchema), kafkaProperties)
        .setStartFromLatest()
        .assignTimestampsAndWatermarks(new OutOfOrdernessTimestampAndWatermarksAssigner(
        inferTimeSize (watermarkSize, watermarkTimeUnit) , timestampField));
    return env.addSource(kafkaSource);
  }

  public DataStream<Map<String, Object>> getSourceKafkaStream(StreamExecutionEnvironment env,
                                                              Map<String, String> propsMap,
                                                              String sourceTopic,
                                                              String timestampField,
                                                              String eventTimeFormat,
                                                              String eventTimeType,
                                                              Integer watermarkSize,
                                                              String watermarkTimeUnit,
                                                              List<String> sourceFieldNames,
                                                              boolean isNestedSchema) throws Exception {

    Properties kafkaProperties = getKafkaProperties(propsMap);
    FlinkKafkaConsumerBase<Map<String, Object>> kafkaSource = new FlinkKafkaConsumer<>(
          sourceTopic, new StringToMapDeserializationSchema(sourceFieldNames, timestampField, eventTimeFormat,
        eventTimeType, isNestedSchema), kafkaProperties)
        .setStartFromLatest()
        .assignTimestampsAndWatermarks(new OutOfOrdernessTimestampAndWatermarksAssigner(
        inferTimeSize (watermarkSize, watermarkTimeUnit) , timestampField));

    return env.addSource(kafkaSource);
  }

  public FeatureStore getFeatureStoreHandle() throws IOException, FeatureStoreException {
    // establish connection to feature store
    // set necessary variables. this is temporary solution until flink is fully integrated with hsfs
    System.setProperty("hopsworks.restendpoint", "https://hopsworks.glassfish.service.consul:8182");
    System.setProperty("hopsworks.domain.truststore", "t_certificate");

    //get handle
    HopsworksConnection connection = HopsworksConnection.builder().build();
    return connection.getFeatureStore();
  }

  private static String readMaterialPassword() throws Exception {
    return FileUtils.readFileToString(new File("material_passwd"));
  }

  public Time inferTimeSize (Integer size, String timeUnit) {
    switch (timeUnit.toLowerCase()) {
      case "milliseconds":
        return size > 0 ? Time.milliseconds(size): null;
      case "seconds":
        return size > 0 ? Time.seconds(size): null;
      case "minutes":
        return size > 0 ? Time.minutes(size): null;
      case "hours":
        return size > 0 ? Time.hours(size): null;
      case "days":
        return size > 0 ? Time.days(size): null;
      default:
        throw new ValueException("Only milliseconds, seconds, minutes, hours and days are accepted as time units");
    }
  }

  public WindowAssigner inferWindowType(String windowType, Time size, Time slide, Time gap){
    switch (windowType.toLowerCase()) {
      case "tumbling":
        if (size == null) {
          throw new ValueException("for tumbling window types window size is required to be more than 0!");
        }
        return TumblingEventTimeWindows.of(size);
      case "sliding":
        if (size == null || slide == null ) {
          throw new ValueException("for sliding window types both window size and slide size are required to be more " +
              "than 0!");
        }
        return SlidingEventTimeWindows.of(size, slide);
      case "session":
        if (gap == null) {
          throw new ValueException("sliding window types slide size is required to be more than 0!");
        }
        return EventTimeSessionWindows.withGap(gap);
      default:
        throw new ValueException("Only tumbling, sliding and session are accepted as window types");
    }
  }
}

