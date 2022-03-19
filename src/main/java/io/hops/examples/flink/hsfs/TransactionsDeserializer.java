package io.hops.examples.flink.hsfs;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/*
public class TransactionsDeserializer implements DeserializationSchema<SourceTransaction>  {
  @Override
  public SourceTransaction deserialize(byte[] message) throws IOException {
    SpecificDatumReader<SourceTransaction> reader = new SpecificDatumReader<>(SourceTransaction.SCHEMA$);
    BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(message), null);
    return reader.read(null, binaryDecoder);
  }
  
  @Override
  public boolean isEndOfStream(SourceTransaction sourceTransaction) {
    return false;
  }
  
  @Override
  public TypeInformation<SourceTransaction> getProducedType() {
    return TypeInformation.of(SourceTransaction.class);
  }
}
*/

public class TransactionsDeserializer implements KafkaDeserializationSchema<SourceTransaction> {
  
  @Override
  public boolean isEndOfStream(SourceTransaction sourceTransaction) {
    return false;
  }
  
  @Override
  public SourceTransaction deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    /*
    byte[] message = consumerRecord.value();
    SpecificDatumReader<SourceTransaction> reader = new SpecificDatumReader<>(SourceTransaction.class);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message,null);
    return reader.read(null,decoder);
     */
    byte[] messageKey = consumerRecord.key();
    byte[] message = consumerRecord.value();
    long offset = consumerRecord.offset();
    long timestamp = consumerRecord.timestamp();
    DeserializationSchema<SourceTransaction> deserializer =
      AvroDeserializationSchema.forSpecific(SourceTransaction.class);
    return deserializer.deserialize(message);
  }
  
  @Override
  public TypeInformation<SourceTransaction> getProducedType() {
    return TypeInformation.of(SourceTransaction.class);
  }
}
