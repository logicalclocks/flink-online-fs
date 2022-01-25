package io.hops.examples.flink.hsfs;

import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.StreamFeatureGroup;

import com.logicalclocks.hsfs.engine.FeatureGroupUtils;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Map;
import java.util.Properties;

public class StreamFeatureGroupExample {

  private FeatureGroupUtils utils = new FeatureGroupUtils();

  public Time inferTimeSize (Integer size, String timeUnit) throws Exception {
    switch (timeUnit.toLowerCase()) {
      case "milliseconds":
        return size > 0 ? Time.milliseconds(size): null;
      case "seconds":
        return size > 0 ? Time.seconds(size): null;
      case "minutes":
        return size > 0 ? Time.minutes(size): null;
      case "hours":
        return size > 0 ? Time.hours(size): null;
      case "days":
        return size > 0 ? Time.days(size): null;
      default:
        throw new Exception("Only milliseconds, seconds, minutes, hours and days are accepted as time units");
    }
  }

  public WindowAssigner inferWindowType(String windowType, Time size, Time slide, Time gap) throws Exception {
    switch (windowType.toLowerCase()) {
      case "tumbling":
        if (size == null) {
          throw new Exception("for tumbling window types window size is required to be more than 0!");
        }
        return TumblingEventTimeWindows.of(size);
      case "sliding":
        if (size == null || slide == null ) {
          throw new Exception("for sliding window types both window size and slide size are required to be more " +
            "than 0!");
        }
        return SlidingEventTimeWindows.of(size, slide);
      case "session":
        if (gap == null) {
          throw new Exception("sliding window types slide size is required to be more than 0!");
        }
        return EventTimeSessionWindows.withGap(gap);
      default:
        throw new Exception("Only tumbling, sliding and session are accepted as window types");
    }
  }

  public void run() throws Exception {

    String keyName = "cc_num";
    String windowType = "tumbling";
    String sourceTopic = "credit_card_transactions";

    /*
    String timestampField = null;
    String eventTimeFormat = null;
    String eventTimeType = null;
    Integer watermarkSize = null;
    String watermarkTimeUnit = null;
     */

    Time maxOutOfOrderness = Time.seconds(60);

    Integer windowSize = 10;
    String windowTimeUnit = "minutes";
    String slideTimeUnit = null;
    Integer slideSize = null;

    Integer gapSize = null;
    String gapTimeUnit = null;

    // define flink env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(30000);

    //get feature store handle
    FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();

    // get streaming feature groups
    StreamFeatureGroup featureGroup = fs.getStreamFeatureGroup("card_transactions_10m_agg", 1);

    Properties kafkaProperties = utils.getKafkaProperties(featureGroup, null);
    kafkaProperties.put("group.id", "console-consumer-myapp");
    
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

    DataStream<Map<String, Object>>
      aggregationStream = env.addSource(transactionFlinkKafkaConsumerBase)
      .rescale()
      .rebalance()
      .keyBy(r -> r.getCcNum())
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .aggregate(new CountAggregate());
    
    featureGroup.insertStream(aggregationStream);
    
    env.execute("Window aggregation of " + windowType);
  }

  public static void main(String[] args) throws Exception {
    StreamFeatureGroupExample streamFeatureGroupExample = new StreamFeatureGroupExample();
    streamFeatureGroupExample.run();
  }
}