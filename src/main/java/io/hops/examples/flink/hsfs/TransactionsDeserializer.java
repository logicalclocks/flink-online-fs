package io.hops.examples.flink.hsfs;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TransactionsDeserializer implements KafkaDeserializationSchema<SourceTransaction> {
  @Override
  public TypeInformation<SourceTransaction> getProducedType() {
    return TypeInformation.of(SourceTransaction.class);
  }
  @Override
  public boolean isEndOfStream(SourceTransaction sourceTransaction) {
    return false;
  }
  @Override
  public SourceTransaction deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
    byte[] message = consumerRecord.value();

    SpecificDatumReader<SourceTransaction> reader = new SpecificDatumReader<>(SourceTransaction.class);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message,null);
    return reader.read(null,decoder);
  }
}
