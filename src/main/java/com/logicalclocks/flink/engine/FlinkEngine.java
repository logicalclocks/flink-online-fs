/*
 *  Copyright (c) 2020-2022. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.flink.engine;

import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.metadata.HopsworksClient;
import com.logicalclocks.base.metadata.HopsworksHttpClient;
import com.logicalclocks.flink.HopsworksConnection;
import com.logicalclocks.flink.StreamFeatureGroup;
import com.logicalclocks.base.engine.FeatureGroupUtils;

import lombok.Getter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class FlinkEngine {
  private static FlinkEngine INSTANCE = null;
  
  public static synchronized FlinkEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new FlinkEngine();
    }
    return INSTANCE;
  }
  
  @Getter
  private StreamExecutionEnvironment streamExecutionEnvironment;
  
  private FeatureGroupUtils utils = new FeatureGroupUtils();
  
  private FlinkEngine() {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    
    // Configure the streamExecutionEnvironment
    streamExecutionEnvironment.getConfig().enableObjectReuse();
    
    // TODO (Davit): allow users to add checkpointing interval
    // streamExecutionEnvironment.enableCheckpointing(30000);
  }
  
  public List<Feature> parseFeatureGroupSchema(ResolvedSchema tableSchema) throws FeatureStoreException {
    List<Feature> features = new ArrayList<>();
    //  corresponds to spark's catalogString()
    for (Column column : tableSchema.getColumns()) {
      Feature f = new Feature(column.getName().toLowerCase(),
        column.getDataType().toString().toLowerCase(), false, false);
      features.add(f);
    }
    return features;
  }
  
  public DataStreamSink<byte[]> writeDataStream(StreamFeatureGroup streamFeatureGroup,
    DataStream<Map<String, Object>> dataStream ,
    Map<String, String> writeOptions)
    throws FeatureStoreException, IOException {
    
    return dataStream
      .map(new OnlineFeatureGroupGenericRecordWriter(streamFeatureGroup.getDeserializedAvroSchema()))
      .rescale()
      .rebalance()
      .addSink(new FlinkKafkaProducer<byte[]>(streamFeatureGroup.getOnlineTopicName(),
        new OnlineFeatureGroupKafkaSink(streamFeatureGroup.getPrimaryKeys().get(0),
          streamFeatureGroup.getOnlineTopicName()),
        //utils.getKafkaProperties(streamFeatureGroup, writeOptions),
        getKafkaProperties(),
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));
  }
  
  public ResolvedSchema sanitizeFeatureNames(ResolvedSchema schema ) throws FeatureStoreException {
    List<Column> sanitizedFeatureNames =
      schema.getColumns().stream().map(f -> Column.physical(f.getName().toLowerCase(), f.getDataType())).collect(
        Collectors.toList());
    
    return ResolvedSchema.of(sanitizedFeatureNames);
  }
  
  private Properties getKafkaProperties() throws FeatureStoreException, IOException {
    HopsworksConnection connection = HopsworksConnection.builder().build();
    HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "broker.kafka.service.consul:9091");
    properties.put("security.protocol", "SSL");
    properties.put("ssl.truststore.location", client.getTrustStorePath());
    properties.put("ssl.truststore.password", client.getCertKey());
    properties.put("ssl.keystore.location", client.getKeyStorePath());
    properties.put("ssl.keystore.password", client.getCertKey());
    properties.put("ssl.key.password", client.getCertKey());
    properties.put("ssl.endpoint.identification.algorithm", "");
    return properties;
  }
}