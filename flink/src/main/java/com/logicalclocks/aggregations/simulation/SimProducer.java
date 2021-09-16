package com.logicalclocks.aggregations.simulation;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import com.logicalclocks.aggregations.utils.Utils;
import com.logicalclocks.taxiDemo.ExerciseBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class SimProducer {
  private Utils utils = new Utils();

  public void run() throws Exception {
    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(ExerciseBase.parallelism);

    DataStream<StoreEvent> simEvens = env.addSource(new EventSimSourceFunction()).keyBy(r -> r.f0).map(new MapFunction<Tuple2<String, StoreEvent>, StoreEvent>() {
      @Override
      public StoreEvent map(Tuple2<String, StoreEvent> storeEvent) throws Exception {
        return storeEvent.f1;
      }
    });

    simEvens.addSink(new FlinkKafkaProducer<StoreEvent>("flink_test_5",
        new AvroKafkaSync("flink_test_5"),
        utils.getKafkaProperties(),
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));;
    env.execute();
  }

  public static void main(String[] args) throws Exception {
    SimProducer simProducer = new SimProducer();
    simProducer.run();
  }
}
