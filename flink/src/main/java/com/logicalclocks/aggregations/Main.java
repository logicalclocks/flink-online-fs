package com.logicalclocks.aggregations;

import com.logicalclocks.aggregations.functions.AggregateRichWindowFunction;
import com.logicalclocks.aggregations.synk.AvroKafkaSink;
import com.logicalclocks.aggregations.utils.Utils;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {

  private Utils utils = new Utils();

  // replace this or provide from command line arguments with your source topic name and kafka broker(s)
  private static final String BROKERS = "broker.kafka.service.consul:9091";
  private static final String SOURCE_TOPIC = "credit_card_transactions";

  public void run() throws Exception {

    // define stream env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // get hsfs handle
    FeatureStore fs = utils.getFeatureStoreHandle();

    // get feature groups
    FeatureGroup tenMinFg = fs.getFeatureGroup("card_transactions_10m_agg", 1);
    FeatureGroup oneHourFg = fs.getFeatureGroup("card_transactions_10m_agg", 1);
    FeatureGroup twelveHFg = fs.getFeatureGroup("card_transactions_10m_agg", 1);

    // get source stream
    DataStream<Map<String, Object>> sourceStream = utils.getSourceKafkaStream(env, SOURCE_TOPIC,
        "datetime", "yyyy-MM-dd hh:mm:ss", "string", 1, "hours");

    // compute 10 min aggregations
    Map<String, Map<String, String>> tenMinFieldsToAggregation = new HashMap<String, Map<String, String>>() {{
      put("avg_amt_per_10m", new HashMap<String, String>() {{put("average", "amount");}});
      put("stdev_amt_per_10m", new HashMap<String, String>() {{put("stdev", "amount");}});
      put("num_trans_per_10m", new HashMap<String, String>() {{put("count", "cc_num");}});
    }};
    List<String> tenMinFgPk = tenMinFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    DataStream<byte[]> tenMinRecord =
        sourceStream.keyBy(r -> r.get(tenMinFgPk.get(0)))
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .apply(new AggregateRichWindowFunction(tenMinFgPk.get(0), tenMinFg.getDeserializedAvroSchema(),
                tenMinFieldsToAggregation));

    //send to online fg topic
    Properties tenMinKafkaProps = utils.getKafkaProperties(tenMinFg);
    tenMinRecord.addSink(new FlinkKafkaProducer<byte[]>(tenMinFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", tenMinFgPk), tenMinFg.getOnlineTopicName()),
        tenMinKafkaProps,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    // compute 1 hour aggregations
    Map<String, Map<String, String>> oneHourFgFieldsToAggregation = new HashMap<String, Map<String, String>>() {{
      put("avg_amt_per_1h", new HashMap<String, String>() {{put("average", "amount");}});
      put("stdev_amt_per_1h", new HashMap<String, String>() {{put("stdev", "amount");}});
      put("num_trans_per_1n", new HashMap<String, String>() {{put("count", "cc_num");}});
    }};

    List<String> oneHourFgPk = oneHourFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    DataStream<byte[]> oneHourRecord =
        sourceStream.keyBy(r -> r.get(oneHourFgPk.get(0)))
            .window(TumblingEventTimeWindows.of(Time.minutes(60)))
            .apply(new AggregateRichWindowFunction(oneHourFgPk.get(0), oneHourFg.getDeserializedAvroSchema(),
                oneHourFgFieldsToAggregation));

    //send to online fg topic
    Properties oneHourKafkaProps = utils.getKafkaProperties(oneHourFg);
    oneHourRecord.addSink(new FlinkKafkaProducer<byte[]>(oneHourFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", oneHourFgPk), oneHourFg.getOnlineTopicName()),
        oneHourKafkaProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    // compute 12 hour aggregations
    Map<String, Map<String, String>> twelveHFieldsToAggregation = new HashMap<String, Map<String, String>>() {{
      put("avg_amt_per_12h", new HashMap<String, String>() {{put("average", "amount");}});
      put("stdev_amt_per_12h", new HashMap<String, String>() {{put("stdev", "amount");}});
      put("num_trans_per_12n", new HashMap<String, String>() {{put("count", "cc_num");}});
    }};

    List<String> twelveHFgPk = twelveHFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    DataStream<byte[]> twelveHRecord =
        sourceStream.keyBy(r -> r.get(twelveHFgPk.get(0)))
            .window(TumblingEventTimeWindows.of(Time.minutes(60 * 12)))
            .apply(new AggregateRichWindowFunction(twelveHFgPk.get(0), twelveHFg.getDeserializedAvroSchema(),
                twelveHFieldsToAggregation));

    //send to online fg topic
    Properties twelveHKafkaProps = utils.getKafkaProperties(twelveHFg);
    twelveHRecord.addSink(new FlinkKafkaProducer<byte[]>(twelveHFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", twelveHFgPk), twelveHFg.getOnlineTopicName()),
        twelveHKafkaProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    Main demo = new Main();
    demo.run();
  }
}
