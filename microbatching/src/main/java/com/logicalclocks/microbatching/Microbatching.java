package com.logicalclocks.microbatching;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.Storage;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.engine.SparkEngine;
import com.logicalclocks.hsfs.engine.Utils;
import com.logicalclocks.hsfs.metadata.HopsworksClient;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class Microbatching {

  private FeatureStore featureStore;
  private String featureGroupName;
  private Integer featureGroupVersion;
  private FeatureGroup featureGroup;
  private Map<String, String> hudiwriteOptions;
  private Map<String, String> writeOptions;

  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private Utils utils = new Utils();

  public Microbatching(String featureGroupName, Integer featureGroupVersion) throws FeatureStoreException {
    this.featureGroupName = featureGroupName;
    this.featureGroupVersion = featureGroupVersion;
  }

  public void run() throws Exception {

    //get handle
    HopsworksConnection connection = HopsworksConnection.builder().build();
    featureStore = connection.getFeatureStore();
    featureGroup = featureStore.getFeatureGroup(featureGroupName, featureGroupVersion);

    hudiwriteOptions = new HashMap<String, String>() {{
        put("hoodie.bulkinsert.shuffle.parallelism", "2");
        put("hoodie.insert.shuffle.parallelism", "2");
        put("hoodie.upsert.shuffle.parallelism", "2");
      }};

    writeOptions = new HashMap<String, String>() {{
        put("startingOffsets", "earliest");
        put("subscribe", featureGroup.getOnlineTopicName());
      }};
    Map<String, String> kafkaOption = featureGroupEngine.getKafkaConfig(featureGroup, writeOptions);

    Dataset<Row> streamingDF = SparkEngine.getInstance().getSparkSession()
        .readStream()
        .format("kafka")
        .options(kafkaOption)
        .load();

    String queryPrefix = "hudi_foreachBatch_";
    String queryName = queryPrefix + featureGroup.getOnlineTopicName() + "_" + LocalDateTime.now().format(
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

    streamingDF.select(org.apache.spark.sql.avro.functions.from_avro(
        org.apache.spark.sql.functions.col("value"),
        featureGroup.getAvroSchema()).alias("value")).select("value.*")
        .writeStream()
        .outputMode("update")
        .foreachBatch((VoidFunction2<Dataset<Row>, Long>) this::streamToHudi)
        .option("checkpointLocation", "/Projects/" + HopsworksClient.getInstance().getProject().getProjectName()
            + "/Resources/" + queryName + "-checkpoint")
        .start()
        .awaitTermination();
  }

  private void streamToHudi(Dataset<Row> df, Long batchId) throws IOException, FeatureStoreException, ParseException {
    df.persist();
    df.printSchema();
    featureGroup.insert(utils.sanitizeFeatureNames(df),
        Storage.OFFLINE, false, HudiOperationType.UPSERT, hudiwriteOptions);
    df.unpersist();
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("featureGroupName", "featureGroupName", true,
        "Feature Group Name");
    options.addOption("featureGroupVersion", "featureGroupVersion", true,
        "Feature Group version");

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    String featureGroupName = commandLine.getOptionValue("featureGroupName");
    Integer featureGroupVersion = Integer.parseInt(commandLine.getOptionValue("featureGroupVersion"));

    Microbatching microbatching = new Microbatching(featureGroupName, featureGroupVersion);
    microbatching.run();
  }
}
