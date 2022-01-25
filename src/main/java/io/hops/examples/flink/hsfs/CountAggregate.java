package io.hops.examples.flink.hsfs;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.HashMap;
import java.util.Map;

public class CountAggregate implements AggregateFunction<SourceTransaction,
  Tuple4<Long, Long, Double, Double>, Map<String, Object>> {

  @Override
  public Tuple4<Long, Long, Double, Double> createAccumulator() {
    return new Tuple4<>(0L, 0L,0.0,0.0);
  }

  @Override
  public Tuple4<Long, Long, Double, Double> add(SourceTransaction record, Tuple4<Long, Long, Double, Double>
    accumulator) {
    return new Tuple4<>(record.getCcNum(), accumulator.f1 + 1, accumulator.f2 + record.getAmount(), 0.0);
  }

  @Override
  public Map<String, Object> getResult(Tuple4<Long, Long, Double, Double> accumulator) {
    return new HashMap<String, Object>() {{
        put("cc_num", accumulator.f0);
        put("num_trans_per_10m", accumulator.f1);
        put("avg_amt_per_10m", accumulator.f2/accumulator.f1);
        put("stdev_amt_per_10m", accumulator.f3);
      }};
  }

  @Override
  public Tuple4<Long, Long, Double, Double> merge(Tuple4<Long, Long, Double, Double> accumulator,
                                                  Tuple4<Long, Long, Double, Double> accumulator1) {
    return new Tuple4<>(accumulator1.f0, accumulator.f1 + accumulator1.f1, accumulator.f2 + accumulator1.f2,
      accumulator.f3 + accumulator1.f3);
  }
}
