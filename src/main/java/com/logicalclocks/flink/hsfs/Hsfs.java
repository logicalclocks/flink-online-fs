package com.logicalclocks.flink.hsfs;

import com.logicalclocks.hsfs.*;
import com.logicalclocks.hsfs.constructor.*;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;

import java.io.IOException;

public class Hsfs {

  public static void main(String[] args) throws IOException, FeatureStoreException {

    System.setProperty("hopsworks.restendpoint", "https://hopsworks.glassfish.service.consul:8182");
    System.setProperty("hopsworks.domain.truststore", "t_certificate");

    HopsworksConnection connection = HopsworksConnection.builder().build();
    FeatureStore fs = connection.getFeatureStore();

  }
}
