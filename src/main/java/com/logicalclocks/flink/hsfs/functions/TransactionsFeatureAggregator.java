package com.logicalclocks.flink.hsfs.functions;

import com.logicalclocks.flink.hsfs.schemas.SourceTransaction;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 *  WindowFunction to compute Transaction aggregates
 */
public class TransactionsFeatureAggregator implements WindowFunction<SourceTransaction,
    Tuple4<Long, Long, Double, Double>, Long, TimeWindow> {

  @Override
  public void apply(Long key, TimeWindow timeWindow, Iterable<SourceTransaction> iterable,
                    Collector<Tuple4<Long, Long, Double, Double>> collector) throws Exception {

    DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

    long cnt = 0;
    for (SourceTransaction r : iterable) {
      cnt++;
      descriptiveStatistics.addValue(r.getAmount());
    }

    double avg = descriptiveStatistics.getSum() / cnt;

    // emit a computed aggregates
    collector.collect(new Tuple4<Long, Long, Double, Double>(key, cnt, avg, descriptiveStatistics.getStandardDeviation()));
  }
}
