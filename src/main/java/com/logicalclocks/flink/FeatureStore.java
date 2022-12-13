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

import com.logicalclocks.base.FeatureStoreBase;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.StorageConnectorBase;
import com.logicalclocks.base.TrainingDatasetBase;
import com.logicalclocks.base.metadata.StorageConnectorApi;

import com.logicalclocks.flink.constructor.Query;
import com.logicalclocks.flink.engine.FeatureViewEngine;
import com.logicalclocks.flink.engine.FeatureGroupEngine;

import com.logicalclocks.flink.metadata.MetaDataUtils;
import lombok.NonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class FeatureStore extends FeatureStoreBase {

  private FeatureGroupEngine featureGroupEngine;
  private MetaDataUtils metaDataUtils = new MetaDataUtils();
  private StorageConnectorApi storageConnectorApi;
  private FeatureViewEngine featureViewEngine;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureStore.class);

  private static final Integer DEFAULT_VERSION = 1;

  public FeatureStore() {
    storageConnectorApi = new StorageConnectorApi();
    featureViewEngine = new FeatureViewEngine();
    featureGroupEngine = new FeatureGroupEngine();
  }
  
  /**
   * Get a feature group object from the feature store.
   *
   * @param name    the name of the feature group
   * @param version the version of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return metaDataUtils.getStreamFeatureGroup(this, name, version);
  }

  /**
   * Get a feature group object with default version `1` from the feature store.
   *
   * @param name the name of the feature group
   * @return FeatureGroup
   * @throws FeatureStoreException
   * @throws IOException
   */
  @Override
  public StreamFeatureGroup getStreamFeatureGroup(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature group `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getStreamFeatureGroup(name, DEFAULT_VERSION);
  }

  @Override
  public StreamFeatureGroup.StreamFeatureGroupBuilder createStreamFeatureGroup() {
    return StreamFeatureGroup.builder()
        .featureStore(this);
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version)
      throws IOException, FeatureStoreException {
    return metaDataUtils.getOrCreateStreamFeatureGroup(this, name, version, null,
        null, null, null, false, null, null);
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          boolean onlineEnabled, String eventTime)
      throws IOException, FeatureStoreException {
    return metaDataUtils.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, null, null, onlineEnabled, null, eventTime);
  }

  @Override
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, List<String> primaryKeys,
                                                          List<String> partitionKeys, boolean onlineEnabled,
                                                          String eventTime) throws IOException, FeatureStoreException {


    return metaDataUtils.getOrCreateStreamFeatureGroup(this, name, version, null,
        primaryKeys, partitionKeys, null, onlineEnabled, null, eventTime);
  }
  
  @Override
  public Object createExternalFeatureGroup() {
    return null;
  }
  
  public StreamFeatureGroup getOrCreateStreamFeatureGroup(String name, Integer version, String description,
                                                          List<String> primaryKeys, List<String> partitionKeys,
                                                          String hudiPrecombineKey, boolean onlineEnabled,
                                                          StatisticsConfig statisticsConfig,
                                                          String eventTime)
      throws IOException, FeatureStoreException {

    return metaDataUtils.getOrCreateStreamFeatureGroup(this, name, version, description,
        primaryKeys, partitionKeys, hudiPrecombineKey, onlineEnabled, statisticsConfig, eventTime);
  }


  public StorageConnector getStorageConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.class);
  }

  public StorageConnector.JdbcConnector getJdbcConnector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.JdbcConnector.class);
  }

  public StorageConnector.JdbcConnector getOnlineStorageConnector() throws FeatureStoreException, IOException {
    return storageConnectorApi.getOnlineStorageConnector(this, StorageConnector.JdbcConnector.class);
  }
  
  @Override
  public Object getGcsConnector(String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  public StorageConnector.S3Connector getS3Connector(String name) throws FeatureStoreException, IOException {
    return storageConnectorApi.getByName(this, name, StorageConnector.S3Connector.class);
  }
  
  @Override
  public Object getRedshiftConnector(String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public Object getSnowflakeConnector(String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public Object getAdlsConnector(String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public Object getKafkaConnector(String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public Object getBigqueryConnector(String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  public StorageConnectorBase.HopsFsConnectorBase getHopsFsConnector(String name) throws FeatureStoreException,
      IOException {
    return storageConnectorApi.getByName(this, name, StorageConnectorBase.HopsFsConnectorBase.class);
  }
  
  @Override
  public Object getExternalFeatureGroups(@NonNull String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public Object sql(String query) {
    return null;
  }
  
  @Override
  public TrainingDatasetBase getTrainingDataset(@NonNull String name, @NonNull Integer version)
    throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public TrainingDatasetBase getTrainingDataset(String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public Object getTrainingDatasets(@NonNull String name) throws FeatureStoreException, IOException {
    return null;
  }
  
  
  public FeatureView.FeatureViewBuilder createFeatureView() {
    return new FeatureView.FeatureViewBuilder(this);
  }

  /**
   * Get feature view metadata object or create a new one if it doesn't exist. This method doesn't update
   * existing feature view metadata object.
   *
   * @param name name of the feature view
   * @param query Query object
   * @param version version of the feature view
   * @return FeatureView
   */
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version)
      throws FeatureStoreException, IOException {
    return featureViewEngine.getOrCreateFeatureView(this, name, version, query, null, null);
  }

  /**
   * Get feature view metadata object or create a new one if it doesn't exist. This method doesn't update
   * existing feature view metadata object.
   *
   * @param name name of the feature view
   * @param query Query object
   * @param version version of the feature view
   * @param description description of the feature view
   * @param labels list of label features
   * @return FeatureView
   */
  public FeatureView getOrCreateFeatureView(String name, Query query, Integer version, String description,
                                            List<String> labels) throws FeatureStoreException, IOException {
    return featureViewEngine.getOrCreateFeatureView(this, name, version, query, description, labels);
  }

  /**
   * Get a feature view object from the selected feature store.
   *
   * @param name    name of the feature view
   * @param version version to get
   * @return FeatureView
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureView getFeatureView(@NonNull String name, @NonNull Integer version)
      throws FeatureStoreException, IOException {
    return featureViewEngine.get(this, name, version);
  }

  /**
   * Get a feature view object with the default version `1` from the selected feature store.
   *
   * @param name name of the feature view
   * @return FeatureView
   * @throws FeatureStoreException
   * @throws IOException
   */
  public FeatureView getFeatureView(String name) throws FeatureStoreException, IOException {
    LOGGER.info("VersionWarning: No version provided for getting feature view `" + name + "`, defaulting to `"
        + DEFAULT_VERSION + "`.");
    return getFeatureView(name, DEFAULT_VERSION);
  }
  
  @Override
  public Object getExternalFeatureGroup(@NonNull String name, @NonNull Integer version)
    throws FeatureStoreException, IOException {
    return null;
  }
  
  @Override
  public Object getExternalFeatureGroup(String name) throws FeatureStoreException, IOException {
    return null;
  }
}
