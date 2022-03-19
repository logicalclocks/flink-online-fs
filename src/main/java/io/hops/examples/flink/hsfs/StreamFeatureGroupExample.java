package io.hops.examples.flink.hsfs;

import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StreamFeatureGroup;

import com.logicalclocks.hsfs.engine.FeatureGroupUtils;

import com.logicalclocks.hsfs.metadata.KafkaApi;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class StreamFeatureGroupExample {

  private FeatureGroupUtils utils = new FeatureGroupUtils();
  private KafkaApi kafkaApi = new KafkaApi();
  
  public void run() throws Exception {

    String windowType = "tumbling";
    String sourceTopic = "credit_card_transactions";
    Duration maxOutOfOrderness = Duration.ofSeconds(60);
    
    // define flink env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.enableCheckpointing(30000);

    //get feature store handle
    FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();

    // get stream feature group
    StreamFeatureGroup featureGroup;
    try {
      featureGroup = fs.getStreamFeatureGroup("card_transactions_10m_agg", 1);
    } catch (java.io.IOException e) {
      featureGroup = (fs.createStreamFeatureGroup()
        .name("card_transactions_10m_agg")
        .description("card_transactions_10m_agg")
        .version(1)
        .primaryKeys(Collections.singletonList("cc_num"))
        .statisticsConfig(new StatisticsConfig(false, false, false, false))
        .build());
  
      ResolvedSchema schema = ResolvedSchema.of(
        Column.physical("cc_num", DataTypes.BIGINT()),
        Column.physical("num_trans_per_10m", DataTypes.BIGINT()),
        Column.physical("avg_amt_per_10m", DataTypes.DOUBLE()),
        Column.physical("stdev_amt_per_10m", DataTypes.DOUBLE()));
      
      featureGroup.save(schema, null);
    }
    
    Properties kafkaProperties = utils.getKafkaProperties(featureGroup, null);

    // define transaction source
    KafkaSource<SourceTransaction> transactionSource = KafkaSource.<SourceTransaction>builder()
      .setProperties(kafkaProperties)
      .setTopics(sourceTopic)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.of(new TransactionsDeserializer()))
      //.setValueOnlyDeserializer(new TransactionsDeserializer())
      .build();
  
    // define watermark strategy
    WatermarkStrategy<SourceTransaction> customWatermark = WatermarkStrategy
      .<SourceTransaction>forBoundedOutOfOrderness(maxOutOfOrderness)
      .withTimestampAssigner((event, timestamp) -> event.getDatetime());
  
    // aggregate stream and return DataStream<Map<String, Object>>
    DataStream<Map<String, Object>>
      aggregationStream = env.fromSource(transactionSource, customWatermark, "Transaction Kafka Source")
      .rescale()
      .rebalance()
      .keyBy(r -> r.getCcNum())
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .aggregate(new CountAggregate());
    
    // insert stream
    featureGroup.insertStream(aggregationStream);
    
    env.execute("Window aggregation of " + windowType);
  }

  public static void main(String[] args) throws Exception {
    StreamFeatureGroupExample streamFeatureGroupExample = new StreamFeatureGroupExample();
    streamFeatureGroupExample.run();
  }
}