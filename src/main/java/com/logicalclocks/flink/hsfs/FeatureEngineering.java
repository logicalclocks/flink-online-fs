package com.logicalclocks.flink.hsfs;

import com.logicalclocks.flink.hsfs.Utils.Utils;
import com.logicalclocks.flink.hsfs.functions.TransactionsFeatureAggregator;
import com.logicalclocks.flink.hsfs.schemas.SourceTransaction;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
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

public class FeatureEngineering {

  private Utils utils = new Utils();

  public void run(String brokers, String sourceTopic, String dbURL, String username, String password) throws Exception {

    // Replace with the correct endpoint based on your setup (ip and project name)
    // Replace with Username and Password
    JDBCOptions.Builder jdbcOptionsBuilder = JDBCOptions.builder()
        .setDBUrl(dbURL)
        .setUsername(username)
        .setPassword(password);

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


    // compute 1 hour aggregations
    DataStream<Tuple4<Long, Long, Double, Double>> oneHourFeatures =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(60)))
            .apply(new TransactionsFeatureAggregator());

    // compute 12 hour aggregations
    DataStream<Tuple4<Long, Long, Double, Double>> twelveHourFeatures =
        sourceStream.keyBy(SourceTransaction::getCcNumber)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(60 * 12)))
            .apply(new TransactionsFeatureAggregator());

    utils.writeOnlineFgs(env, jdbcOptionsBuilder, tenMinFeatures, oneHourFeatures, twelveHourFeatures);
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

    options.addOption(Option.builder("dbURL")
        .argName("dbURL")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("username")
        .argName("username")
        .required(true)
        .hasArg()
        .build());

    options.addOption(Option.builder("password")
        .argName("password")
        .required(true)
        .hasArg()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    FeatureEngineering demo = new FeatureEngineering();
    demo.run(commandLine.getOptionValue("brokers"), commandLine.getOptionValue("sourceTopic"),
        commandLine.getOptionValue("dbURL"), commandLine.getOptionValue("username"),
        commandLine.getOptionValue("password"));
  }
}
