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

import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.base.SecurityProtocol;
import com.logicalclocks.base.SslEndpointIdentificationAlgorithm;
import com.logicalclocks.base.StorageConnectorBase;
import com.logicalclocks.base.StorageConnectorType;
import com.logicalclocks.base.metadata.Option;
import com.logicalclocks.base.metadata.StorageConnectorApi;
import com.logicalclocks.base.util.Constants;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "storageConnectorType", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = StorageConnector.HopsFsConnector.class, name = "HOPSFS"),
    @JsonSubTypes.Type(value = StorageConnector.S3Connector.class, name = "S3"),
    @JsonSubTypes.Type(value = StorageConnector.JdbcConnector.class, name = "JDBC"),
    @JsonSubTypes.Type(value = StorageConnector.KafkaConnector.class, name = "KAFKA"),
})

public abstract class StorageConnector extends StorageConnectorBase {
  @Getter @Setter
  protected StorageConnectorType storageConnectorType;

  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private Integer featurestoreId;

  protected StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  
  public StorageConnector refetch() throws FeatureStoreException, IOException {
    return storageConnectorApi.get(getFeaturestoreId(), getName(), StorageConnector.class);
  }

  @JsonIgnore
  @Override
  public String getPath(String subPath) throws FeatureStoreException {
    return null;
  }

  @Override
  public Map<String, String> sparkOptions() throws IOException {
    return null;
  }

  public static class S3Connector extends StorageConnector {

    @Getter @Setter
    private String accessKey;

    @Getter @Setter
    private String secretKey;

    @Getter @Setter
    private String serverEncryptionAlgorithm;

    @Getter @Setter
    private String serverEncryptionKey;

    @Getter @Setter
    private String bucket;

    @Getter @Setter
    private String sessionToken;

    @Getter @Setter
    private String iamRole;

    @JsonIgnore
    public String getPath(String subPath) {
      return "s3://" + bucket + "/"  + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    @Override
    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }
  
    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
      return null;
    }
  
    public void update() throws FeatureStoreException, IOException {
      S3Connector updatedConnector = (S3Connector) refetch();
      this.accessKey = updatedConnector.getAccessKey();
      this.secretKey = updatedConnector.getSecretKey();
      this.sessionToken = updatedConnector.getSessionToken();
    }
  }

  public static class HopsFsConnector extends StorageConnector {

    @Getter @Setter
    private String hopsfsPath;

    @Getter @Setter
    private String datasetName;

    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }
  
    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
      return null;
    }
  
    @JsonIgnore
    public String getPath(String subPath) {
      return hopsfsPath + "/" + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }
  }
  
  public static class JdbcConnector extends StorageConnector {

    @Getter @Setter
    private String connectionString;

    @Getter @Setter
    private List<Option> arguments;

    @Override
    public Map<String, String> sparkOptions() {
      Map<String, String> readOptions = arguments.stream()
          .collect(Collectors.toMap(arg -> arg.getName(), arg -> arg.getValue()));
      readOptions.put(Constants.JDBC_URL, connectionString);
      return readOptions;
    }
  
    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
      return null;
    }
  
    public void update() throws FeatureStoreException, IOException {
      JdbcConnector updatedConnector = (JdbcConnector) refetch();
      this.connectionString = updatedConnector.getConnectionString();
      this.arguments = updatedConnector.getArguments();
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }

  public static class KafkaConnector extends StorageConnector {

    public static final String sparkFormat = "kafka";

    @Getter @Setter
    private String bootstrapServers;

    @Getter @Setter
    private SecurityProtocol securityProtocol;

    @Getter
    private String sslTruststoreLocation;

    @Getter @Setter
    private String sslTruststorePassword;

    @Getter
    private String sslKeystoreLocation;

    @Getter @Setter
    private String sslKeystorePassword;

    @Getter @Setter
    private String sslKeyPassword;

    @Getter @Setter
    private SslEndpointIdentificationAlgorithm sslEndpointIdentificationAlgorithm;

    @Getter @Setter
    private List<Option> options;

    /*
    public void setSslTruststoreLocation(String sslTruststoreLocation) {
      this.sslTruststoreLocation = SparkEngine.getInstance().addFile(sslTruststoreLocation);
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
      this.sslKeystoreLocation = SparkEngine.getInstance().addFile(sslKeystoreLocation);
    }
    */

    @Override
    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();
      options.put(Constants.KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);
      options.put(Constants.KAFKA_SECURITY_PROTOCOL, securityProtocol.toString());
      if (!Strings.isNullOrEmpty(sslTruststoreLocation)) {
        options.put(Constants.KAFKA_SSL_TRUSTSTORE_LOCATION, sslTruststoreLocation);
      }
      if (!Strings.isNullOrEmpty(sslTruststorePassword)) {
        options.put(Constants.KAFKA_SSL_TRUSTSTORE_PASSWORD, sslTruststorePassword);
      }
      if (!Strings.isNullOrEmpty(sslKeystoreLocation)) {
        options.put(Constants.KAFKA_SSL_KEYSTORE_LOCATION, sslKeystoreLocation);
      }
      if (!Strings.isNullOrEmpty(sslKeystorePassword)) {
        options.put(Constants.KAFKA_SSL_KEYSTORE_PASSWORD, sslKeystorePassword);
      }
      if (!Strings.isNullOrEmpty(sslKeyPassword)) {
        options.put(Constants.KAFKA_SSL_KEY_PASSWORD, sslKeyPassword);
      }
      // can be empty string
      if (sslEndpointIdentificationAlgorithm != null) {
        options.put(
            Constants.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, sslEndpointIdentificationAlgorithm.getValue());
      }
      if (this.options != null && !this.options.isEmpty()) {
        Map<String, String> argOptions = this.options.stream()
            .collect(Collectors.toMap(Option::getName, Option::getValue));
        options.putAll(argOptions);
      }
      return options;
    }
  
    @Override
    public Object read(String query, String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
      return null;
    }
  
  
    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }

    public Object readStream(String topic, boolean topicPattern, String messageFormat, String schema,
                             Map<String, String> options, boolean includeMetadata) throws FeatureStoreException,
        IOException {
      if (!Arrays.asList("avro", "json", null).contains(messageFormat.toLowerCase())) {
        throw new IllegalArgumentException("Can only read JSON and AVRO encoded records from Kafka.");
      }

      if (topicPattern) {
        options.put("subscribePattern", topic);
      } else {
        options.put("subscribe", topic);
      }
      
      return null;
      /*
      return SparkEngine.getInstance().readStream(this, sparkFormat, messageFormat.toLowerCase(),
          schema, options, includeMetadata);
       */
    }
  }
  
}
