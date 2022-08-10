package io.hops.examples.flink.nyctaxi;

import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.ByteArrayOutputStream;

public class TaxiFareEventKafkaSync implements SerializationSchema<TaxiFare> {
  @SneakyThrows
  @Override
  public byte[] serialize(TaxiFare taxiFareEvent) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    Schema schema = ReflectData.get().getSchema(TaxiFare.class);
    DatumWriter<TaxiFare> dataFileWriter = new ReflectDatumWriter<>(schema);
    dataFileWriter.write(taxiFareEvent, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
