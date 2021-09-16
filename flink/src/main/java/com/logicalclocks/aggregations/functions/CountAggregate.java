package com.logicalclocks.aggregations.functions;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CountAggregate implements AggregateFunction<Map<String, Object>, Tuple6<Long, Long, Long, Long, Long, Long>,
    Tuple6<Long, Long, Long, Long, Long, Long>>  {

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> createAccumulator() {
    return new Tuple6<>(0L,0L,0L,0L,0L,0L);
  }

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> add(Map<String, Object> record,
                                                        Tuple6<Long, Long, Long, Long, Long, Long> accumulator) {
    return new Tuple6<>(accumulator.f0 + 1, Instant.now().toEpochMilli(), (long) record.get("received_ts"),
        (long) record.get("deserializationTimestamp"), (long) record.get("kafkaCommitTimestamp"), 0L);
  }

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> getResult(Tuple6<Long, Long, Long, Long, Long, Long> accumulator) {
    return new Tuple6<>(accumulator.f0, accumulator.f1, accumulator.f2, accumulator.f3, accumulator.f4, accumulator.f5);
  }

  @Override
  public Tuple6<Long, Long, Long, Long, Long, Long> merge(Tuple6<Long, Long, Long, Long, Long, Long> accumulator,
                                                          Tuple6<Long, Long, Long, Long, Long, Long> accumulator1) {
    return new Tuple6<>(accumulator.f0 + accumulator1.f0,
        accumulator1.f1,
        accumulator1.f2,
        accumulator1.f3,
        accumulator1.f4,
        accumulator1.f5);
  }

  public static class MyRichWindowFunction extends RichWindowFunction<Tuple6<Long, Long, Long, Long, Long, Long>,
      byte[], Object, TimeWindow> {

    // whether to output window end or not
    private String primaryKeyName;

    // TODO (davit): why not schema directly?
    // Avro schema in JSON format.
    private final String schemaString;

    // Cannot be serialized so we create these in open().
    private transient Schema schema;
    private transient GenericData.Record record;

    public MyRichWindowFunction(String primaryKeyName, Schema schema){
      this.primaryKeyName = primaryKeyName;
      // TODO (davit): why not schema directly?
      this.schemaString = schema.toString();
    }

    @Override
    public void apply(Object key, TimeWindow timeWindow, Iterable<Tuple6<Long, Long, Long, Long, Long, Long>> iterable,
                      Collector<byte[]> collector) throws Exception {

      Tuple6<Long, Long, Long, Long, Long, Long> agg =  iterable.iterator().next();


      record.put(primaryKeyName, key);
      record.put("count_added_to_bag", agg.f0);
      record.put("aggregation_start_time", agg.f1);
      record.put("max_event_timestamp", agg.f2);
      record.put("max_deserialization_timestamp", agg.f3);
      record.put("max_kafka_event_commit_timestamp", agg.f4);

      // Just a dummy values here, online fs will overwrite this values
      record.put("bench_commit_time",  Instant.now().toEpochMilli());
      record.put("kafka_timestamp",  Instant.now().toEpochMilli());

      // window end
      record.put("end",  timeWindow.getEnd());

      // here it ends
      record.put("aggregation_end_time",  Instant.now().toEpochMilli());
      collector.collect(encode(record));
    }

    @Override
    public void open(Configuration parameters) {
      this.schema = new Schema.Parser().parse(this.schemaString);
      this.record = new GenericData.Record(this.schema);

    }

    private byte[] encode(GenericRecord record) throws IOException {

      List<GenericRecord> records = new ArrayList<>();
      records.add(record);
      GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byteArrayOutputStream.reset();
      BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
      for(GenericRecord segment: records) {
        datumWriter.write(segment, binaryEncoder);
      }
      binaryEncoder.flush();
      byte[] bytes = byteArrayOutputStream.toByteArray();
      return bytes;

      /*
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<StoreEvent> dataFileWriter = new SpecificDatumWriter<StoreEvent>(storeEvent.getSchema());
      dataFileWriter.write(storeEvent, encoder);
      encoder.flush();
      out.toByteArray();
       */
    }

    private Long processingDelay(long eventTimeStamp) {
      return (Instant.now().toEpochMilli() - eventTimeStamp);
    }

  }
}
