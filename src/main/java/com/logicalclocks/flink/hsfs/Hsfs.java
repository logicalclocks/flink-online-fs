package com.logicalclocks.flink.hsfs;

import com.logicalclocks.hsfs.*;
import com.logicalclocks.hsfs.constructor.*;

import java.io.IOException;

public class Hsfs {
  public static void main(String[] args) throws IOException, FeatureStoreException {
    HopsworksConnection connection = HopsworksConnection.builder().build();
    FeatureStore fs = connection.getFeatureStore();
  }
}
