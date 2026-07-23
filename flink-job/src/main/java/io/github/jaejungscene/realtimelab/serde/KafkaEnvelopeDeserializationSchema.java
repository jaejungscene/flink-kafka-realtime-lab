package io.github.jaejungscene.realtimelab.serde;

import io.github.jaejungscene.realtimelab.model.KafkaRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public final class KafkaEnvelopeDeserializationSchema
        implements KafkaRecordDeserializationSchema<KafkaRecord> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRecord> out) {
        out.collect(new KafkaRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                decode(record.key()),
                decode(record.value())));
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }

    private static String decode(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
