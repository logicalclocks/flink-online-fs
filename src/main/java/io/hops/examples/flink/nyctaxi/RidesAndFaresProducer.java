package io.hops.examples.flink.nyctaxi;

import io.hops.examples.flink.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class RidesAndFaresProducer {
  private final Utils utils = new Utils();
  
  public void run() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(ExerciseBase.parallelism);
  
    String taxiRidesTopic = "taxi_rides_topic";
    String taxiFareTopic = "taxi_fare_topic";
    //Properties kafkaProperties = utils.getKafkaProperties();
    
     /*
    // rides
    SingleOutputStreamOperator<String> ridesStream =
      env.addSource(ExerciseBase.rideSourceOrTest(new TaxiRideGenerator()))
        .keyBy((TaxiRide ride) -> ride.rideId)
        .filter((TaxiRide ride) -> ride.isStart)
        .map(new MapFunction<TaxiRide, String>() {
          @Override
          public String map(TaxiRide taxiRide) throws Exception {
            //for csv only
            return taxiRide.toString();
          }
        });
  
    // fares
    SingleOutputStreamOperator<String> faresStream =
      env.addSource(ExerciseBase.fareSourceOrTest(new TaxiFareGenerator()))
        .keyBy((TaxiFare fare) -> fare.rideId)
        .map(new MapFunction<TaxiFare, String>() {
          @Override
          public String map(TaxiFare fare) throws Exception {
            //for csv only
            return fare.toString();
          }
        });
        
    StreamingFileSink<String> ridesCsvSink = StreamingFileSink
      .forRowFormat(new Path("hdfs:///Projects/fraud_demo/Resources/rides.csv"), new SimpleStringEncoder<String>("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build();
  
    StreamingFileSink<String> faresCsvSink = StreamingFileSink
      .forRowFormat(new Path("hdfs:///Projects/fraud_demo/Resources/fares.csv"), new SimpleStringEncoder<String>("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build();
  
    ridesStream.sinkTo((Sink<String, ?, ?, ?>) ridesCsvSink);
    faresStream.sinkTo((Sink<String, ?, ?, ?>) faresCsvSink);
      */
  
  
    // rides
    SingleOutputStreamOperator<Tuple11<Long, Boolean, Long, Long, Float, Float, Float, Float, Integer, Long, Long>> ridesStream =
      env.addSource(ExerciseBase.rideSourceOrTest(new TaxiRideGenerator()))
        .keyBy((TaxiRide ride) -> ride.rideId)
        .filter((TaxiRide ride) -> ride.isStart)
        .map(new MapFunction<TaxiRide, Tuple11<Long, Boolean, Long, Long, Float, Float, Float, Float, Integer, Long, Long>>() {
          @Override
          public Tuple11<Long, Boolean, Long, Long, Float, Float, Float, Float, Integer, Long, Long> map(TaxiRide taxiRide) throws Exception {
            //for csv only
            return taxiRide.toTuple();
          }
        });
  
    // fares
    SingleOutputStreamOperator<Tuple8<Long, Long, Long, Long, String, Float, Float, Float>> faresStream =
      env.addSource(ExerciseBase.fareSourceOrTest(new TaxiFareGenerator()))
        .keyBy((TaxiFare fare) -> fare.rideId)
        .map(new MapFunction<TaxiFare, Tuple8<Long, Long, Long, Long, String, Float, Float, Float>>() {
          @Override
          public Tuple8<Long, Long, Long, Long, String, Float, Float, Float> map(TaxiFare fare) throws Exception {
            //for csv only
            return fare.toTuple();
          }
        });
    
    faresStream.writeAsCsv("hdfs:///Projects/fraud_demo/Resources/fares_a.csv", FileSystem.WriteMode.OVERWRITE, "\n",
      ",");
    ridesStream.writeAsCsv("hdfs:///Projects/fraud_demo/Resources/rides_a.csv", FileSystem.WriteMode.OVERWRITE, "\n",
      ",");

    /*
    KafkaSink<String> ridesSink = KafkaSink.<String>builder()
      .setKafkaProducerConfig(kafkaProperties)
      .setBootstrapServers(kafkaProperties.getProperty("bootstrap.servers"))
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(taxiRidesTopic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build();
  
    KafkaSink<String> faresSink = KafkaSink.<String>builder()
      .setKafkaProducerConfig(kafkaProperties)
      .setBootstrapServers(kafkaProperties.getProperty("bootstrap.servers"))
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(taxiFareTopic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build();
  
    ridesStream.sinkTo(ridesSink);
    faresStream.sinkTo(faresSink);
     */
  
    env.execute();
  }

  public static void main(String[] args) throws Exception {
    RidesAndFaresProducer ridesAndFaresFeatureGroupProducer =  new RidesAndFaresProducer();
    ridesAndFaresFeatureGroupProducer.run();
  }
}
