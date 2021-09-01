package com.logicalclocks.flink.taxiDemo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import static com.logicalclocks.flink.taxiDemo.ExerciseBase.fareSourceOrTest;
import static com.logicalclocks.flink.taxiDemo.ExerciseBase.printOrTest;
import static com.logicalclocks.flink.taxiDemo.ExerciseBase.rideSourceOrTest;

public class RidesAndFaresJoined {
  public static void main(String[] args) throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(ExerciseBase.parallelism);

    DataStream<TaxiRide> rides =
        env.addSource(rideSourceOrTest(new TaxiRideGenerator()))
            .filter((TaxiRide ride) -> ride.isStart)
            .keyBy((TaxiRide ride) -> ride.rideId);

    DataStream<TaxiFare> fares =
        env.addSource(fareSourceOrTest(new TaxiFareGenerator()))
            .keyBy((TaxiFare fare) -> fare.rideId);

    // Set a UID on the stateful flatmap operator so we can read its state using the State
    // Processor API.
    DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides =
        rides.connect(fares).flatMap(new EnrichmentFunction()).uid("enrichment");

    printOrTest(enrichedRides);


    env.execute("Join Rides with Fares (java RichCoFlatMap)");
  }

  public static class EnrichmentFunction
      extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
    // keyed, managed state
    private ValueState<TaxiRide> rideState;
    private ValueState<TaxiFare> fareState;

    @Override
    public void open(Configuration config) {
      rideState =
          getRuntimeContext()
              .getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
      fareState =
          getRuntimeContext()
              .getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
    }

    @Override
    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out)
        throws Exception {
      TaxiFare fare = fareState.value();
      if (fare != null) {
        fareState.clear();
        out.collect(Tuple2.of(ride, fare));
      } else {
        rideState.update(ride);
      }
    }

    @Override
    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out)
        throws Exception {
      TaxiRide ride = rideState.value();
      if (ride != null) {
        rideState.clear();
        out.collect(Tuple2.of(ride, fare));
      } else {
        fareState.update(fare);
      }
    }
  }
}
