package com.logicalclocks.flink.hsfs.schemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString

public class OneHourAggregatedTransactions {
  @Getter
  @Setter
  @JsonProperty("cc_num")
  private Long cc_num;

  @Getter
  @Setter
  @JsonProperty("num_trans_per_1h")
  private Long num_trans_per_1h;

  @Getter
  @Setter
  @JsonProperty("avg_amt_per_1h")
  private Double avg_amt_per_1h;

  @Getter
  @Setter
  @JsonProperty("stdev_amt_per_1h")
  private Double stdev_amt_per_1h;
}
