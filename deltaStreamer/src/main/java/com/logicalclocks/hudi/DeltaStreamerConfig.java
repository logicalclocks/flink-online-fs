/*
 * Copyright (c) 2021 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hudi;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

public class DeltaStreamerConfig implements Serializable {

  private HoodieDeltaStreamer.Config deltaStreamerConfig(Map<String, String> hudiWriteOpts,
                                                         Integer minSyncIntervalSeconds,
                                                         Map<String, String> writeOptions) {

    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();

    // base path for the target hoodie table.
    cfg.targetBasePath = hudiWriteOpts.get("targetBasePath");
    // name of the target table
    cfg.targetTableName = hudiWriteOpts.get("targetTableName");

    // hudi table type
    cfg.tableType = hudiWriteOpts.get(HudiEngine.HUDI_TABLE_STORAGE_TYPE);

    // Takes one of these values : UPSERT (default), INSERT
    cfg.operation = WriteOperationType.UPSERT;

    // Enable syncing to hive metastore
    cfg.enableHiveSync = true;

    // Subclass of org.apache.hudi.utilities.sources to read data
    cfg.sourceClassName = HudiEngine.KAFKA_SOURCE;

    // subclass of org.apache.hudi.utilities.schema.SchemaProvider to attach schemas to input & target table data,
    cfg.schemaProviderClassName = HudiEngine.SCHEMA_PROVIDER;

    if (minSyncIntervalSeconds != null) {
      // the min sync interval of each sync in continuous mode
      cfg.minSyncIntervalSeconds = minSyncIntervalSeconds;
    } else {
      // Delta Streamer runs in continuous mode running source-fetch -> Transform -> Hudi Write in loop
      cfg.continuousMode = true;
    }

    // A subclass or a list of subclasses of org.apache.hudi.utilities.transform.Transformer. Allows transforming raw
    // source Dataset to a target Dataset (conforming to target schema) before writing. Default : Not set.
    // E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which allows a SQL query templated to be
    // passed as a transformation function). Pass a comma-separated list of subclass names to chain the transformations
    cfg.transformerClassNames = new ArrayList<String>() {{
        add(HudiEngine.DELTA_STREAMER_TRANSFORMER);
      }};

    // setup checkpoint
    cfg.checkpoint = hudiWriteOpts.get("checkpointLocation");
    cfg.initialCheckpointProvider = HudiEngine.INITIAL_CHECKPOINT_PROVIDER;

    // Field within source record to decide how to break ties between records with same key in input data.
    cfg.sourceOrderingField =  hudiWriteOpts.get("sourceOrderingField");

    cfg.configs = new ArrayList<String>() {{
        // hudi featute group properties
        add(HudiEngine.HUDI_RECORD_KEY + "=" + hudiWriteOpts.get(HudiEngine.HUDI_RECORD_KEY));
        add(HudiEngine.HUDI_PARTITION_FIELD + "=" + hudiWriteOpts.get(HudiEngine.HUDI_PARTITION_FIELD));
        add(HudiEngine.HUDI_KEY_GENERATOR_OPT_KEY + "=" + HudiEngine.HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);

        add("projectId" + "=" + hudiWriteOpts.get("projectId"));
        add("featureStoreName" + "=" + hudiWriteOpts.get("featureStoreName"));
        add("featureGroupName" + "=" + hudiWriteOpts.get("featureGroupName"));
        add("featureGroupVersion" + "=" + hudiWriteOpts.get("featureGroupVersion"));
        add(HudiEngine.HUDI_BASE_PATH + "=" + hudiWriteOpts.get("featureGroupLocation"));
        add(HudiEngine.HUDI_KAFKA_TOPIC + "=" + hudiWriteOpts.get("onlineTopicName"));
        add(HudiEngine.FEATURE_GROUP_SCHEMA + "=" + hudiWriteOpts.get("avroSchema"));

        add(HudiEngine.HUDI_HIVE_SYNC_PARTITION_FIELDS + "="
            + hudiWriteOpts.get(HudiEngine.HUDI_HIVE_SYNC_PARTITION_FIELDS));
        add(HudiEngine.HUDI_PRECOMBINE_FIELD + "=" +  hudiWriteOpts.get(HudiEngine.HUDI_PRECOMBINE_FIELD));

        add(HudiEngine.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY + "="
            + HudiEngine.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL);

        // User provided options
        writeOptions.entrySet().stream().filter(e -> !e.getKey().startsWith("kafka."))
            .forEach(e -> add(e.getKey() + "=" + e.getValue()));
        // Kafka props
        writeOptions.entrySet().stream().filter(e -> e.getKey().startsWith("kafka."))
            .forEach(e -> add(e.getKey().replace("kafka.", "") + "=" + e.getValue()));
      }};

    return cfg;
  }

  public void streamToHoodieTable(Map<String, String> hudiWriteOpts, Map<String, String> writeOptions,
                                  Integer minSyncIntervalSeconds, SparkSession spark) throws Exception {
    HoodieDeltaStreamer.Config cfg = deltaStreamerConfig(hudiWriteOpts, minSyncIntervalSeconds,
        writeOptions);
    try {
      HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg,
          JavaSparkContext.fromSparkContext(spark.sparkContext()));
      HoodieDeltaStreamer.DeltaSyncService deltaSyncService = deltaStreamer.getDeltaSyncService();
      deltaSyncService.start((error) -> {
        deltaSyncService.close();
        return true;
      });
      deltaSyncService.waitForShutdown();
    } catch (IOException e) {
      throw new Exception(e.toString());
    }
  }
}
