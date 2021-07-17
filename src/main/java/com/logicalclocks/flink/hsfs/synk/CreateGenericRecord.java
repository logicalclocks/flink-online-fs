package com.logicalclocks.flink.hsfs.synk;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.List;

public class CreateGenericRecord implements MapFunction<Tuple4<Long, Long, Double, Double>, GenericRecord> {

  private Schema schema;
  private List<String> fields;

  public CreateGenericRecord(Schema schema, List<String> fields) {
    this.schema = schema;
    this.fields = fields;
  }

  @Override
  public GenericRecord map(Tuple4<Long, Long, Double, Double> aggregations) throws Exception {
    final GenericData.Record record = new GenericData.Record(this.schema);
    record.put(fields.get(0), aggregations.getField(0));
    record.put(fields.get(1), aggregations.getField(1));
    record.put(fields.get(2), aggregations.getField(2));
    record.put(fields.get(3), aggregations.getField(3));
    return record;
  }
}
