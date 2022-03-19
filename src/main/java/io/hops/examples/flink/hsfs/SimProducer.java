package io.hops.examples.flink.hsfs;


import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;

public class SimProducer {
  
  private Properties getKafkaProperties(String topic) throws FeatureStoreException, IOException {
    HopsworksConnection connection = HopsworksConnection.builder().build();
    HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "broker.kafka.service.consul:9091");
    properties.put("security.protocol", "SSL");
    properties.put("ssl.truststore.location", client.getTrustStorePath());
    properties.put("ssl.truststore.password", client.getCertKey());
    properties.put("ssl.keystore.location", client.getKeyStorePath());
    properties.put("ssl.keystore.password", client.getCertKey());
    properties.put("ssl.key.password", client.getCertKey());
    properties.put("ssl.endpoint.identification.algorithm", "");
    properties.put("topic", topic);
    return properties;
  }
  
  public void run(String topicName, Integer batchSize) throws Exception {
    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    
    DataStream<SourceTransaction> simEvens =
      env.addSource(new TransactionEventSimulator(batchSize)).keyBy(r -> r.getCcNum());
  
    Properties kafkaCinfig = getKafkaProperties(topicName);
    KafkaSink<SourceTransaction> sink = KafkaSink.<SourceTransaction>builder()
      .setKafkaProducerConfig(kafkaCinfig)
      .setBootstrapServers(kafkaCinfig.getProperty("bootstrap.servers"))
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(topicName)
        .setValueSerializationSchema(new TransactionEventKafkaSync())
        .build()
      )
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build();
    
    simEvens.sinkTo(sink);
    
    env.execute();
  }
  
  public static void main(String[] args) throws Exception {
    
    Options options = new Options();
    
    options.addOption(Option.builder("topicName")
      .argName("topicName")
      .required(true)
      .hasArg()
      .build());
    
    options.addOption(Option.builder("batchSize")
      .argName("batchSize")
      .required(true)
      .hasArg()
      .build());
    
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);
    
    String topicName = commandLine.getOptionValue("topicName");
    Integer batchSize = Integer.parseInt(commandLine.getOptionValue("batchSize"));
    
    SimProducer simProducer = new SimProducer();
    simProducer.run(topicName, batchSize);
  }
}
