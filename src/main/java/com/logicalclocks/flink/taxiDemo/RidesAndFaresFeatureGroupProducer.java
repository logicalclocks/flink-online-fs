package com.logicalclocks.flink.taxiDemo;

import com.logicalclocks.flink.hsfs.synk.AvroKafkaSink;
import com.logicalclocks.flink.hsfs.utils.Utils;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.logicalclocks.flink.taxiDemo.ExerciseBase.fareSourceOrTest;
import static com.logicalclocks.flink.taxiDemo.ExerciseBase.rideSourceOrTest;

public class RidesAndFaresFeatureGroupProducer {
  private Utils utils = new Utils();

  public void run() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(ExerciseBase.parallelism);

    // get hsfs handle
    FeatureStore fs = utils.getFeatureStoreHandle();

    // rides
    // get feature groups
    FeatureGroup taxiRideFg = fs.getFeatureGroup("ride_fg", 1);
    List<String> taxiRideKeys = taxiRideFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    DataStream<byte[]> rides =
        env.addSource(rideSourceOrTest(new TaxiRideGenerator()))
            .filter((TaxiRide ride) -> ride.isStart)
            .keyBy((TaxiRide ride) -> ride.rideId)
            .map( new MapToFeatureGroupRecord(taxiRideKeys.get(0), taxiRideFg.getDeserializedAvroSchema()));
            //.map(new MapToFeatureGroupRecord(taxiRideKeys.get(0), taxiRideFg.getDeserializedAvroSchema()));
            //.returns(Types.LIST(Types.BYTE));
    //send to online fg topic
    Properties taxiRideFgKafkaPropertiies = utils.getKafkaProperties(taxiRideFg);
    rides.addSink(new FlinkKafkaProducer<byte[]>(taxiRideFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", taxiRideKeys), taxiRideFg.getOnlineTopicName()),
        taxiRideFgKafkaPropertiies,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

    /*
    // fares
    // get feature groups
    FeatureGroup taxiFareFg = fs.getFeatureGroup("fare_fg", 1);
    List<String> taxiFareKeys = taxiFareFg.getFeatures().stream().filter(Feature::getPrimary)
        .map(Feature::getName).collect(Collectors.toList());
    DataStream<byte[]> fares =
        env.addSource(fareSourceOrTest(new TaxiFareGenerator()))
            .keyBy((TaxiFare fare) -> fare.rideId)
            .map(new MapToFeatureGroupRecord(taxiFareKeys.get(0), taxiFareFg.getDeserializedAvroSchema()))
            .returns(Types.LIST(Types.BYTE));

    //send to online fg topic
    Properties taxiFareFgKafkaPropertiies = utils.getKafkaProperties(taxiFareFg);
    fares.addSink(new FlinkKafkaProducer<byte[]>(taxiFareFg.getOnlineTopicName(),
        new AvroKafkaSink(String.join(",", taxiFareKeys), taxiFareFg.getOnlineTopicName()),
        taxiFareFgKafkaPropertiies,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));
     */

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    RidesAndFaresFeatureGroupProducer ridesAndFaresFeatureGroupProducer =  new RidesAndFaresFeatureGroupProducer();
    ridesAndFaresFeatureGroupProducer.run();
  }
}
