package io.hops.examples.flink.hsfs;

import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.StreamFeatureGroup;

import com.logicalclocks.hsfs.engine.FeatureGroupUtils;

import com.logicalclocks.hsfs.metadata.KafkaApi;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Map;
import java.util.Properties;

public class StreamFeatureGroupExample {

  private FeatureGroupUtils utils = new FeatureGroupUtils();
  private KafkaApi kafkaApi = new KafkaApi();
  
  public void run() throws Exception {

    String windowType = "tumbling";
    String sourceTopic = "credit_card_transactions";
    Time maxOutOfOrderness = Time.seconds(60);
    
    // define flink env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(30000);

    //get feature store handle
    FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();

    // get stream feature group
    StreamFeatureGroup featureGroup = fs.getStreamFeatureGroup("card_transactions_10m_agg", 1);

    Properties kafkaProperties = utils.getKafkaProperties(featureGroup, null);
    
    // get raw data from the source
    FlinkKafkaConsumerBase<SourceTransaction> transactionFlinkKafkaConsumerBase =
      new FlinkKafkaConsumer<>(sourceTopic, new TransactionsDeserializer(), kafkaProperties)
        .setStartFromLatest()
        .assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor<SourceTransaction>(maxOutOfOrderness){
            @Override
            public long extractTimestamp(SourceTransaction element) {
              return element.getDatetime();
            }
          });

    // aggregate stream and return  DataStream<Map<String, Object>>
    DataStream<Map<String, Object>>
      aggregationStream = env.addSource(transactionFlinkKafkaConsumerBase)
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