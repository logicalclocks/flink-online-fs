package com.logicalclocks.flink.hsfs.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.metadata.KafkaApi;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
   * @return the DataStream object
   * @throws Exception
   */

  public DataStream<Map<String, Object>> getSourceKafkaStream(StreamExecutionEnvironment env,
                                                              String sourceTopic,
                                                              String timestampField,
                                                              String eventTimeFormat,
                                                              String eventTimeType)
      throws Exception {

    Properties kafkaProperties = getKafkaProperties();
    FlinkKafkaConsumerBase<Map<String, Object>> kafkaSource = new FlinkKafkaConsumer<>(
        sourceTopic, new StringToMapDeserializationSchema(), kafkaProperties).setStartFromEarliest();

    kafkaSource.setStartFromEarliest();
    kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Map<String, Object>>() {
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
    });
    return env.addSource(kafkaSource);
  }

  /**
   * Setup the Kafka source stream.
   *
   * The Kafka topic is populated by the same producer notebook.
   * The stream at this stage contains just string.
   *
   * @param env The Stream execution environment to which add the source
   * @param propsMap Kafka properties parsed from config file
   * @param sourceTopic the Kafka topic to read the data from
   * @return the DataStream object
   * @throws Exception
   */

  public DataStream<Map<String, Object>> getSourceKafkaStream(StreamExecutionEnvironment env,
                                                              Map<String, String> propsMap,
                                                              String sourceTopic,
                                                              String timestampField,
                                                              String eventTimeFormat,
                                                              String eventTimeType) throws Exception {

    Properties kafkaProperties = getKafkaProperties(propsMap);
    FlinkKafkaConsumerBase<Map<String, Object>> kafkaSource = new FlinkKafkaConsumer<>(
        sourceTopic, new StringToMapDeserializationSchema(), kafkaProperties).setStartFromEarliest();

    kafkaSource.setStartFromEarliest();
    kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Map<String, Object>>() {
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
    });

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

  public static class StringToMapDeserializationSchema implements DeserializationSchema<Map<String, Object>> {

    @Override
    public Map<String, Object> deserialize(byte[] message) throws IOException {
      /*
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        JsonNode jsonNodeRoot = objectMapper.readTree(element);
        JsonNode jsonNodeDatetimeField = null;
        if (jsonNodeRoot.has(timestampField)){
          jsonNodeDatetimeField = jsonNodeRoot.get(timestampField);
        } else {
          throw new VerifyError("Provided field doesn't exist");
        }
       */
      // convert JSON string to Java Map
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(message, Map.class);
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
  }
}
