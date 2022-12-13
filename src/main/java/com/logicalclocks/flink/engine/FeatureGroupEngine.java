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

import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.JobConfiguration;
import com.logicalclocks.base.engine.FeatureGroupUtils;
import com.logicalclocks.base.metadata.FeatureGroupApi;

import com.logicalclocks.flink.StreamFeatureGroup;
import com.logicalclocks.flink.metadata.MetaDataUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureGroupEngine {

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private MetaDataUtils metaDataUtils = new MetaDataUtils();
  private FeatureGroupUtils utils = new FeatureGroupUtils();
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroupEngine.class);
  
  @SneakyThrows
  public void insertStream(StreamFeatureGroup streamFeatureGroup, DataStream<Map<String, Object>> featureData,
    List<String> partitionKeys, String hudiPrecombineKey, Map<String, String> writeOptions,
    JobConfiguration jobConfiguration) {

    if (writeOptions == null) {
      writeOptions = new HashMap<>();
    }

    /*
    if (streamFeatureGroup.getId() == null) {
      streamFeatureGroup = saveFeatureGroupMetaData(streamFeatureGroup, partitionKeys, hudiPrecombineKey, writeOptions,
          jobConfiguration, featureData);
    }
    FlinkEngine.getInstance().writeDataStream(streamFeatureGroup,
      FlinkEngine.getInstance().sanitizeFeatureNames(featureData), writeOptions);
     */
    
    FlinkEngine.getInstance().writeDataStream(streamFeatureGroup, featureData, writeOptions);
  }
  
  public StreamFeatureGroup saveFeatureGroupMetaData(StreamFeatureGroup featureGroup, List<String> partitionKeys,
                                                     String hudiPrecombineKey, Map<String, String> writeOptions,
                                                     JobConfiguration sparkJobConfiguration,
    ResolvedSchema featureData)
      throws FeatureStoreException, IOException {

    if (featureGroup.getFeatures() == null) {
      featureGroup.setFeatures(FlinkEngine.getInstance()
          .parseFeatureGroupSchema(FlinkEngine.getInstance().sanitizeFeatureNames(featureData)));
    }

    LOGGER.info("Featuregroup features: " + featureGroup.getFeatures());

    // verify primary, partition, event time or hudi precombine keys
    utils.verifyAttributeKeyNames(featureGroup, partitionKeys, hudiPrecombineKey);

    StreamFeatureGroup apiFG = (StreamFeatureGroup) featureGroupApi.saveFeatureGroupMetaData(featureGroup,
        partitionKeys, hudiPrecombineKey, writeOptions, sparkJobConfiguration, StreamFeatureGroup.class);
    featureGroup.setOnlineTopicName(apiFG.getOnlineTopicName());

    return featureGroup;
  }
}
