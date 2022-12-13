/*
 *  Copyright (c) 2022-2022. Hopsworks AB
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

package com.logicalclocks.flink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.base.DeltaStreamerJobConf;
import com.logicalclocks.base.EntityEndpointType;
import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.JobConfiguration;
import com.logicalclocks.base.engine.CodeEngine;
import com.logicalclocks.base.metadata.FeatureGroupBase;

import com.logicalclocks.flink.constructor.Query;

import com.logicalclocks.flink.engine.FeatureGroupEngine;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase {

  @Getter
  @Setter
  @JsonIgnore
  private FeatureStore featureStore;

  @Getter
  @Setter
  private Boolean onlineEnabled;

  @Getter
  @Setter
  private StorageConnector onlineStorageConnector;

  @Getter
  @Setter
  private StorageConnector offlineStorageConnector;

  @Getter
  @Setter
  private String type = "streamFeatureGroupDTO";

  @Getter
  @Setter
  protected String location;

  @Getter
  @Setter
  private List<String> statisticColumns;

  @JsonIgnore
  // These are only used in the client. In the server they are aggregated in the `features` field
  private List<String> partitionKeys;

  @JsonIgnore
  // This is only used in the client. In the server they are aggregated in the `features` field
  private String hudiPrecombineKey;
  
  private StatisticsConfig statisticsConfig;
  
  @Getter(onMethod = @__(@Override))
  @Setter(onMethod = @__(@Override))
  private String onlineTopicName;

  @Setter
  private DeltaStreamerJobConf deltaStreamerJobConf;
  
  public final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private final CodeEngine codeEngine = new CodeEngine(EntityEndpointType.FEATURE_GROUP);

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamFeatureGroup.class);

  @Builder
  public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                            List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                            boolean onlineEnabled, List<Feature> features,
                            StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime) {
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
        ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
        ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = hudiPrecombineKey != null ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.onlineTopicName = onlineTopicName;
    this.eventTime = eventTime;
  }

  public StreamFeatureGroup() {
  }

  // used for updates
  public StreamFeatureGroup(Integer id, String description, List<Feature> features) {
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public StreamFeatureGroup(FeatureStore featureStore, int id) {
    this.featureStore = featureStore;
    this.id = id;
  }
  
  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException
   * @throws ParseException
   */
  public Query asOf(String wallclockTime) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime);
  }

  /**
   * Get Query object to retrieve all features of the group at a point in the past.
   * This method selects all features in the feature group and returns a Query object
   * at the specified point in time. This can then either be read into a Dataframe
   * or used further to perform joins or construct a training dataset.
   *
   * @param wallclockTime Datetime string. The String should be formatted in one of the
   *     following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.
   * @param excludeUntil Datetime string. The String should be formatted in one of the
   *     following formats `%Y%m%d`, `%Y%m%d%H`, `%Y%m%d%H%M`, or `%Y%m%d%H%M%S`.
   * @return Query. The query object with the applied time travel condition
   * @throws FeatureStoreException
   * @throws ParseException
   */
  public Query asOf(String wallclockTime, String excludeUntil) throws FeatureStoreException, ParseException {
    return selectAll().asOf(wallclockTime, excludeUntil);
  }

  public void insertStream(DataStream<Map<String, Object>> featureData) {
    insertStream(featureData, null, null);
  }
  
  public void insertStream(DataStream<Map<String, Object>> featureData, Map<String, String> writeOptions,
    JobConfiguration jobConfiguration) {
    featureGroupEngine.insertStream(this, featureData,  partitionKeys, hudiPrecombineKey, writeOptions,
        jobConfiguration);
  }
  
  @JsonIgnore
  public String getAvroSchema() throws FeatureStoreException, IOException {
    return getSubject().getSchema();
  }

  @JsonIgnore
  @Override
  public List<String> getComplexFeatures() {
    return utils.getComplexFeatures(features);
  }

  @JsonIgnore
  @Override
  public String getFeatureAvroSchema(String featureName) throws FeatureStoreException, IOException {
    return utils.getFeatureAvroSchema(featureName, utils.getDeserializedAvroSchema(getAvroSchema()));
  }

  @JsonIgnore
  @Override
  public String getEncodedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getEncodedAvroSchema(getDeserializedAvroSchema(), utils.getComplexFeatures(features));
  }

  @JsonIgnore
  @Override
  public Schema getDeserializedAvroSchema() throws FeatureStoreException, IOException {
    return utils.getDeserializedAvroSchema(getAvroSchema());
  }

  @Override
  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  @Override
  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  @Override
  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  @Override
  public Query selectExceptFeatures(List<Feature> features) {
    List<String> exceptFeatures = features.stream().map(Feature::getName).collect(Collectors.toList());
    return selectExcept(exceptFeatures);
  }

  @Override
  public Query selectExcept(List<String> features) {
    return new Query(this,
        getFeatures().stream().filter(f -> !features.contains(f.getName())).collect(Collectors.toList()));
  }
}
