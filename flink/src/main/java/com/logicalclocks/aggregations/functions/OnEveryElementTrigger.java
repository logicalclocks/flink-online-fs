package com.logicalclocks.aggregations.functions;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class OnEveryElementTrigger extends Trigger<Object, TimeWindow> {
  private static final long serialVersionUID = 1L;

  public OnEveryElementTrigger() {
  }

  public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
    ctx.registerEventTimeTimer(window.maxTimestamp());
    return TriggerResult.FIRE;
  }

  public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
    return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
  }

  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
    return TriggerResult.CONTINUE;
  }

  public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }

  public boolean canMerge() {
    return true;
  }

  public void onMerge(TimeWindow window, OnMergeContext ctx) {
    long windowMaxTimestamp = window.maxTimestamp();
    if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
      ctx.registerEventTimeTimer(windowMaxTimestamp);
    }

  }

  public String toString() {
    return "EventTimeTrigger()";
  }

  public static OnEveryElementTrigger create() {
    return new OnEveryElementTrigger();
  }
}

