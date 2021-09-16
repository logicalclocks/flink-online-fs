package com.logicalclocks.aggregations.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class CountTrigger<W extends Window> extends Trigger<Object, W> {
  private static final long serialVersionUID = 1L;
  private final long maxCount;
  private final ReducingStateDescriptor<Long> stateDesc;

  public CountTrigger(long maxCount) {
    this.stateDesc = new ReducingStateDescriptor("count", new CountTrigger.Sum(), LongSerializer.INSTANCE);
    this.maxCount = maxCount;
  }

  public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
    ReducingState<Long> count = (ReducingState)ctx.getPartitionedState(this.stateDesc);
    count.add(1L);
    if ((Long)count.get() >= this.maxCount) {
      count.clear();
      return TriggerResult.FIRE;
    } else {
      return TriggerResult.CONTINUE;
    }
  }

  public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
    return TriggerResult.CONTINUE;
  }

  public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
    return TriggerResult.CONTINUE;
  }

  public void clear(W window, TriggerContext ctx) throws Exception {
    ((ReducingState)ctx.getPartitionedState(this.stateDesc)).clear();
  }

  public boolean canMerge() {
    return true;
  }

  public void onMerge(W window, OnMergeContext ctx) throws Exception {
    ctx.mergePartitionedState(this.stateDesc);
  }

  public String toString() {
    return "CountTrigger(" + this.maxCount + ")";
  }

  public static <W extends Window> CountTrigger<W> of(long maxCount) {
    return new CountTrigger(maxCount);
  }

  private static class Sum implements ReduceFunction<Long> {
    private static final long serialVersionUID = 1L;

    private Sum() {
    }

    public Long reduce(Long value1, Long value2) throws Exception {
      return value1 + value2;
    }
  }
}
