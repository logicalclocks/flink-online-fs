package com.logicalclocks.hudi;

import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.engine.FeatureGroupEngine;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;

public class DeltaStreamerJob {
  private final FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private final FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();
  private final HudiEngine hudiEngine = new HudiEngine();

  private FeatureStore featureStore;
  private FeatureGroup featureGroup;

  private String featureGroupName;
  private Integer featureGroupVersion;
  private Integer minSyncIntervalSeconds;

  public DeltaStreamerJob(String featureGroupName, Integer featureGroupVersion, Integer minSyncIntervalSeconds) {
    this.featureGroupName = featureGroupName;
    this.featureGroupVersion = featureGroupVersion;
    this.minSyncIntervalSeconds = minSyncIntervalSeconds;
  }

  public void run() throws Exception {

    //get handle
    HopsworksConnection connection = HopsworksConnection.builder().build();

    featureStore = connection.getFeatureStore();

    featureGroup = featureGroupApi.getFeatureGroup(this.featureStore, featureGroupName, featureGroupVersion);
    hudiEngine.streamToHoodieTable(featureGroup, minSyncIntervalSeconds,
        featureGroupEngine.getKafkaConfig(featureGroup, null));
  }

  public static void main(String[] args) throws Exception {

    Options options = new Options();
    options.addOption("featureGroupName", "featureGroupName", true,
        "Feature Group Name");
    options.addOption("featureGroupVersion", "featureGroupVersion", true,
        "Feature Group version");
    options.addOption("minSyncIntervalSeconds", "minSyncIntervalSeconds", true,
        "minSyncIntervalSeconds");

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    String featureGroupName = commandLine.getOptionValue("featureGroupName");
    Integer featureGroupVersion = Integer.parseInt(commandLine.getOptionValue("featureGroupVersion"));
    Integer minSyncIntervalSeconds = Integer.parseInt(commandLine.getOptionValue("minSyncIntervalSeconds"));

    DeltaStreamerJob deltaStreamerJob = new DeltaStreamerJob(featureGroupName, featureGroupVersion,
        minSyncIntervalSeconds);
    deltaStreamerJob.run();
  }
}
