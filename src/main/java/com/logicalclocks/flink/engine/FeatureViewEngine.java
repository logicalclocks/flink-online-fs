/*
 *  Copyright (c) 2022. Hopsworks AB
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.logicalclocks.base.EntityEndpointType;
import com.logicalclocks.base.Feature;
import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.FeatureViewBase;
import com.logicalclocks.base.TrainingDatasetFeature;
import com.logicalclocks.base.constructor.Join;
import com.logicalclocks.base.engine.FeatureViewEngineBase;
import com.logicalclocks.base.metadata.FeatureViewApi;
import com.logicalclocks.base.metadata.TagsApi;

import com.logicalclocks.flink.FeatureView;
import com.logicalclocks.flink.constructor.Query;
import com.logicalclocks.flink.FeatureStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureViewEngine extends FeatureViewEngineBase {

  private FeatureViewApi featureViewApi = new FeatureViewApi();
  private TagsApi tagsApi = new TagsApi(EntityEndpointType.FEATURE_VIEW);
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewEngine.class);

  public FeatureView save(FeatureView featureView) throws FeatureStoreException, IOException {
    FeatureView updatedFeatureView = featureViewApi.save(featureView, FeatureView.class);
    featureView.setVersion(updatedFeatureView.getVersion());
    featureView.setFeatures(updatedFeatureView.getFeatures());
    return featureView;
  }

  public FeatureView update(FeatureView featureView) throws FeatureStoreException,
      IOException {
    featureViewApi.update(featureView, FeatureView.class);
    return featureView;
  }

  public FeatureView get(FeatureStore featureStore, String name,
                         Integer version)
      throws FeatureStoreException, IOException {
    FeatureView featureView = (FeatureView) super.get(featureStore, name, version, FeatureView.class);
    featureView.setFeatureStore(featureStore);
    return featureView;
  }

  public List<FeatureViewBase> get(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    List<FeatureViewBase> featureViewBases = super.get(featureStore, name);
    List<FeatureView> featureView = new ArrayList<>();
    for (FeatureViewBase fvBase : featureViewBases) {
      FeatureView fv = (FeatureView) fvBase;
      fv.setFeatureStore(featureStore);
      fv.getFeatures().stream()
          .filter(f -> f.getFeatureGroup() != null)
          .forEach(f -> f.getFeatureGroup().setFeatureStore(featureStore));
      fv.getQuery().getLeftFeatureGroup().setFeatureStore(featureStore);
      fv.setLabels(
          fv.getFeatures().stream()
              .filter(TrainingDatasetFeature::getLabel)
              .map(TrainingDatasetFeature::getName)
              .collect(Collectors.toList()));
    }
    return featureViewBases;
  }

  public void delete(FeatureStore featureStore, String name) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name);
  }

  public void delete(FeatureStore featureStore, String name, Integer version) throws FeatureStoreException,
      IOException {
    featureViewApi.delete(featureStore, name, version);
  }
  
  private Date getStartTime() {
    return new Date(1000);
  }

  private Date getEndTime() {
    return new Date();
  }
  

  public void addTag(FeatureView featureView, String name, Object value)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureView, name, value);
  }

  public void addTag(FeatureView featureView, String name, Object value, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.add(featureView, trainingDataVersion, name, value);
  }

  public void deleteTag(FeatureView featureView, String name)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureView, name);
  }

  public void deleteTag(FeatureView featureView, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    tagsApi.deleteTag(featureView, trainingDataVersion, name);
  }

  public Object getTag(FeatureView featureView, String name)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, name);
  }

  public Object getTag(FeatureView featureView, String name, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, trainingDataVersion, name);
  }

  public Map<String, Object> getTags(FeatureView featureView)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView);
  }

  public Map<String, Object> getTags(FeatureView featureView, Integer trainingDataVersion)
      throws FeatureStoreException, IOException {
    return tagsApi.get(featureView, trainingDataVersion);
  }

  public FeatureView getOrCreateFeatureView(FeatureStore featureStore, String name, Integer version,  Query query,
                                            String description, List<String> labels)
      throws FeatureStoreException, IOException {
    FeatureView featureView = null;
    try {
      featureView = (FeatureView) get(featureStore, name, version, FeatureView.class);
    } catch (IOException | FeatureStoreException e) {
      if (e.getMessage().contains("Error: 404") && e.getMessage().contains("\"errorCode\":270181")) {
        featureView = new FeatureView.FeatureViewBuilder(featureStore)
            .name(name)
            .version(version)
            .query(query)
            .description(description)
            .labels(labels)
            .build();
      }
    }
    return featureView;
  }
}
