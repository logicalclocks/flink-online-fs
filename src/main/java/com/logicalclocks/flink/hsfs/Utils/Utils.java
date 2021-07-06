package com.logicalclocks.flink.hsfs.Utils;

import com.logicalclocks.flink.hsfs.schemas.OneHourAggregatedTransactions;
import com.logicalclocks.flink.hsfs.schemas.SourceTransaction;
import com.logicalclocks.flink.hsfs.schemas.SourceTransactionSchema;
import com.logicalclocks.flink.hsfs.schemas.TenMinAggregatedTransactions;
import com.logicalclocks.flink.hsfs.schemas.TwelveHourAggregatedTransactions;
import com.logicalclocks.flink.hsfs.synk.AvroKafkaSink;
import com.sun.org.apache.bcel.internal.generic.ARETURN;
import lombok.Generated;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.common.protocol.types.Field;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.BIGINT;

public class Utils {

  public Properties getKafkaProperties(String brokers) throws Exception {

    Properties dataKafkaProps = new Properties();
    String materialPasswd = readMaterialPassword();
    // Replace this the list of your brokers, even better if you make it configurable from the job arguments
    dataKafkaProps.setProperty("bootstrap.servers", brokers);
    // These settings are static and they don't need to be changed
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");

    return dataKafkaProps;
  }

  /**
   * Setup the Kafka source stream.
   *
   * The Kafka topic is populated by the same producer notebook.
   * The stream at this stage contains just string.
   *
   * @param env The Stream execution environment to which add the source
   * @param brokers the list of brokers to use as bootstrap servers for Kafka
   * @param sourceTopic the Kafka topic to read the data from
   * @return the DataStream object
   * @throws Exception
   */

  public DataStream<SourceTransaction> getSourceKafkaStream(StreamExecutionEnvironment env,
                                                            String brokers, String sourceTopic) throws Exception {

    FlinkKafkaConsumerBase<SourceTransaction> kafkaSource = new FlinkKafkaConsumer<>(
        sourceTopic, new SourceTransactionSchema(), getKafkaProperties(brokers)).setStartFromEarliest();
    kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SourceTransaction>() {
      @Override
      public long extractAscendingTimestamp(SourceTransaction element) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Long timeStamp = null;
        try {
          timeStamp = dateFormat.parse(element.getDatetime()).getTime();
        } catch (ParseException e) {
          e.printStackTrace();
        }
        return timeStamp;
      }
    });
    return env.addSource(kafkaSource);
  }

  private static String readMaterialPassword() throws Exception {
    return FileUtils.readFileToString(new File("material_passwd"));
  }

  public GenericRecord parseAvroSchema (String userSchema) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    return avroRecord;
  }

  public class TransactioonsAvroSchema implements MapFunction<SourceTransaction, org.apache.avro.generic.GenericRecord> {
    private String userSchema;

    public TransactioonsAvroSchema(String userSchema) {
      this.userSchema = userSchema;
    }

    GenericRecord avroRecord = parseAvroSchema(this.userSchema);

    @Override
    public org.apache.avro.generic.GenericRecord map(SourceTransaction sourceTransaction) throws Exception {
      avroRecord.put("tid", sourceTransaction.getTid());
      avroRecord.put("datetime", sourceTransaction.getDatetime());
      avroRecord.put("cc_num", sourceTransaction.getCcNumber());
      avroRecord.put("amount", sourceTransaction.getAmount());
      return avroRecord;
    }
  }

  public DataStream<GenericRecord> getKafkaAvroSynk (String brokers, String topic,
                                                     DataStream<GenericRecord> synkDataStream) throws Exception {
    synkDataStream.addSink(new FlinkKafkaProducer<GenericRecord>(topic,
        new AvroKafkaSink("tid"), getKafkaProperties(brokers), FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
    return synkDataStream;
  }

  /**
   * This method takes in input the Stream produced by the method above and write the output
   * in an online feature group.
   * The code already takes care of doing an upsert, so if a key already exists it is updated.
   * @param env
   * @param jdbcOptionsBuilder
   * @param featureGroup
   * @param aggregations
   */
  public void writeOnlineFg(StreamExecutionEnvironment env, JDBCOptions.Builder jdbcOptionsBuilder,
                            String featureGroup, DataStream<Tuple4<Long, Long, Double, Double>> aggregations) {

    Table t = null;

    // Define the fields and the keys (keys should be the none you specified when creating the feature group)
    String fieldNames = null;
    String[] keys = {"cc_num"};;
    String[] fields = null;

    JDBCOptions options = jdbcOptionsBuilder.setTableName(featureGroup).build();
    // Create a table representation of the data stream
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    if (featureGroup.equals("card_transactions_10m_agg_1")) {
      DataStream<TenMinAggregatedTransactions> fgStream = aggregations.map(new MapFunction<Tuple4<Long, Long, Double, Double>, TenMinAggregatedTransactions>() {
        @Override
        public TenMinAggregatedTransactions map(Tuple4<Long, Long, Double, Double> input) throws Exception {
          TenMinAggregatedTransactions tenMinAggregatedTransactions = new TenMinAggregatedTransactions();
          tenMinAggregatedTransactions.setCc_num(input.f0);
          tenMinAggregatedTransactions.setNum_trans_per_10m(input.f1);
          tenMinAggregatedTransactions.setAvg_amt_per_10m(input.f2);
          tenMinAggregatedTransactions.setAvg_amt_per_10m(input.f3);
          return tenMinAggregatedTransactions;
        }
      });
      fieldNames = "cc_num,num_trans_per_10m,avg_amt_per_10m,stdev_amt_per_10m";
      // Register the stream as a flink table
      t = tEnv.fromDataStream(fgStream, fieldNames);
    } else if (featureGroup.equals("card_transactions_1h_agg_1")) {
      DataStream<OneHourAggregatedTransactions> fgStream = aggregations.map(new MapFunction<Tuple4<Long, Long, Double, Double>, OneHourAggregatedTransactions>() {
        @Override
        public OneHourAggregatedTransactions map(Tuple4<Long, Long, Double, Double> input) {
          OneHourAggregatedTransactions oneHourAggregatedTransactions = new OneHourAggregatedTransactions();
          oneHourAggregatedTransactions.setCc_num(input.f0);
          oneHourAggregatedTransactions.setNum_trans_per_1h(input.f1);
          oneHourAggregatedTransactions.setAvg_amt_per_1h(input.f2);
          oneHourAggregatedTransactions.setAvg_amt_per_1h(input.f3);
          return oneHourAggregatedTransactions;
        }
      });
      fieldNames = "cc_num,num_trans_per_1h,avg_amt_per_1h,stdev_amt_per_1h";
      // Register the stream as a flink table
      t = tEnv.fromDataStream(fgStream, fieldNames);
    } else if (featureGroup.equals("card_transactions_12h_agg_1")) {
      DataStream<TwelveHourAggregatedTransactions> fgStream = aggregations.map(new MapFunction<Tuple4<Long, Long, Double, Double>, TwelveHourAggregatedTransactions>() {
        @Override
        public TwelveHourAggregatedTransactions map(Tuple4<Long, Long, Double, Double> input) {
          TwelveHourAggregatedTransactions twelveHourAggregatedTransactions = new TwelveHourAggregatedTransactions();
          twelveHourAggregatedTransactions.setCc_num(input.f0);
          twelveHourAggregatedTransactions.setNum_trans_per_12h(input.f1);
          twelveHourAggregatedTransactions.setAvg_amt_per_12h(input.f2);
          twelveHourAggregatedTransactions.setAvg_amt_per_12h(input.f3);
          return twelveHourAggregatedTransactions;
        }
      });
      fieldNames = "cc_num,num_trans_per_12h,avg_amt_per_12h,stdev_amt_per_12h";
      // Register the stream as a flink table
      t = tEnv.fromDataStream(fgStream, fieldNames);
    }

    tEnv.registerTable(featureGroup, t);
    fields = fieldNames.split(",");

    // Configure and register the JDBC sink
    JDBCUpsertTableSink tablesink = JDBCUpsertTableSink.builder()
        .setOptions(options)
        .setTableSchema(TableSchema.builder().fields(
            fields, new DataType[] {BIGINT(), BIGINT(), DOUBLE(), DOUBLE()}).build())
        .setFlushIntervalMills(1)
        .build();
    tablesink.setKeyFields(keys);
    tablesink.setIsAppendOnly(false);

    tEnv.registerTableSink("online_fg", tablesink);

    // Do the insert
    // The GROUP BY is required to be able to do the upsert on the primary keys
    tEnv.sqlUpdate("INSERT INTO online_fg SELECT " + fieldNames + " FROM " + featureGroup +" GROUP BY " + fieldNames);
  }

  public void writeOnlineFgs(StreamExecutionEnvironment env, JDBCOptions.Builder jdbcOptionsBuilder,
                              DataStream<Tuple4<Long, Long, Double, Double>> tenm,
                              DataStream<Tuple4<Long, Long, Double, Double>> oneh,
                              DataStream<Tuple4<Long, Long, Double, Double>> twelveh) {

    writeOnlineFg(env, jdbcOptionsBuilder, "card_transactions_10m_agg_1", tenm);
    writeOnlineFg(env, jdbcOptionsBuilder, "card_transactions_1h_agg_1", oneh);
    writeOnlineFg(env, jdbcOptionsBuilder, "card_transactions_12h_agg_1", twelveh);
  }
}
