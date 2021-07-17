package com.logicalclocks.flink.hsfs;

import com.logicalclocks.flink.hsfs.synk.AvroKafkaSink;
import com.logicalclocks.flink.hsfs.synk.CreateGenericRecord;
import com.logicalclocks.flink.hsfs.utils.Utils;
import com.logicalclocks.flink.hsfs.functions.TransactionsFeatureAggregator;
import com.logicalclocks.flink.hsfs.schemas.SourceTransaction;

import org.apache.avro.generic.GenericRecord;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Arrays;
import java.util.List;

public class FeatureEngineering {

  private Utils utils = new Utils();

  public void run(String brokers, String sourceTopic) throws Exception {

    // define stream env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // get source stream
    DataStream<SourceTransaction> sourceStream = utils.getSourceKafkaStream(env, brokers, sourceTopic);

    // compute 10 min aggregations
    DataStream<Tuple4<Long, Long, Double, Double>> tenMinFeatures =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(10)))
            .apply(new TransactionsFeatureAggregator());

    // Get generic record for 1 hour aggregation fg
    String tenMinschema = "{\"type\":\"record\",\"name\":\"card_transactions_10m_agg_1\",\"namespace\":\"transaction_featurestore.db\",\"fields\":[{\"name\":\"cc_num\",\"type\":[\"null\",\"long\"]},{\"name\":\"num_trans_per_10m\",\"type\":[\"null\",\"long\"]},{\"name\":\"avg_amt_per_10m\",\"type\":[\"null\",\"double\"]},{\"name\":\"stdev_amt_per_10m\",\"type\":[\"null\",\"double\"]}]}";
    DataStream<GenericRecord> tenMinRecord =
        tenMinFeatures.map(
            new CreateGenericRecord(tenMinschema,
                Arrays.asList("cc_num","num_trans_per_10m","avg_amt_per_10m","stdev_amt_per_10m"))
        );

    //send to online fg topic
    String tenMinFgTopic = "119_14_card_transactions_10m_agg_1_onlinefs";
    List<String> tenMinFgPk = Arrays.asList("cc_num");
    tenMinRecord.addSink(new FlinkKafkaProducer<GenericRecord>(tenMinFgTopic,
        new AvroKafkaSink(tenMinschema, String.join(",", tenMinFgPk), tenMinFgTopic), utils.getKafkaProperties(brokers),
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    // compute 1 hour aggregations
    DataStream<Tuple4<Long, Long, Double, Double>> oneHourFeatures =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(60)))
            .apply(new TransactionsFeatureAggregator());
    // Get generic record for 1 hour aggregation fg
    String oneHourSchema = "{\"type\":\"record\",\"name\":\"card_transactions_1h_agg_1\",\"namespace\":\"transaction_featurestore.db\",\"fields\":[{\"name\":\"cc_num\",\"type\":[\"null\",\"long\"]},{\"name\":\"num_trans_per_1h\",\"type\":[\"null\",\"long\"]},{\"name\":\"avg_amt_per_1h\",\"type\":[\"null\",\"double\"]},{\"name\":\"stdev_amt_per_1h\",\"type\":[\"null\",\"double\"]}]}";
    DataStream<GenericRecord> oneHourRecord =
        oneHourFeatures.map(new CreateGenericRecord(oneHourSchema,
            Arrays.asList("cc_num","num_trans_per_1h","avg_amt_per_1h","stdev_amt_per_1h")));
    //send to online fg topic
    String oneHourFgTopic = "119_15_card_transactions_1h_agg_1_onlinefs";
    List<String> oneHourFgPk = Arrays.asList("cc_num");
    oneHourRecord.addSink(new FlinkKafkaProducer<GenericRecord>(oneHourFgTopic,
        new AvroKafkaSink(oneHourSchema, String.join(",", oneHourFgPk), oneHourFgTopic), utils.getKafkaProperties(brokers),
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    // compute 12 hour aggregations
    DataStream<Tuple4<Long, Long, Double, Double>> twelveHourFeatures =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(60 * 12)))
            .apply(new TransactionsFeatureAggregator());
    // Get generic record for 12 hour aggregation fg
    String twelveHourSchema = "{\"type\":\"record\",\"name\":\"card_transactions_12h_agg_1\",\"namespace\":\"transaction_featurestore.db\",\"fields\":[{\"name\":\"cc_num\",\"type\":[\"null\",\"long\"]},{\"name\":\"num_trans_per_12h\",\"type\":[\"null\",\"long\"]},{\"name\":\"avg_amt_per_12h\",\"type\":[\"null\",\"double\"]},{\"name\":\"stdev_amt_per_12h\",\"type\":[\"null\",\"double\"]}]}";
    DataStream<GenericRecord> twelveHRecord =
        twelveHourFeatures.map(new CreateGenericRecord(twelveHourSchema,
            Arrays.asList("cc_num","num_trans_per_12h","avg_amt_per_12h","stdev_amt_per_12h")));
    //send to online fg topic
    String twelveHFgTopic = "119_16_card_transactions_12h_agg_1_onlinefs";
    List<String> twelveHFgPk = Arrays.asList("cc_num");;
    twelveHRecord.addSink(new FlinkKafkaProducer<GenericRecord>(twelveHFgTopic,
        new AvroKafkaSink(twelveHourSchema, String.join(",", twelveHFgPk), twelveHFgTopic), utils.getKafkaProperties(brokers),
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(Option.builder("brokers")
        .argName("brokers")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("sourceTopic")
        .argName("sourceTopic")
        .required(true)
        .hasArg()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    FeatureEngineering demo = new FeatureEngineering();
    demo.run(commandLine.getOptionValue("brokers"), commandLine.getOptionValue("sourceTopic"));
    //-brokers broker.kafka.service.consul:9091 -sourceTopic credit_card_transactions
  }
}
