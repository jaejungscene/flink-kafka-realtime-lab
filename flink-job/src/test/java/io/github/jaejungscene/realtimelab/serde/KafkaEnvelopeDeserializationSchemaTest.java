package io.github.jaejungscene.realtimelab.serde;

import io.github.jaejungscene.realtimelab.model.KafkaRecord;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaEnvelopeDeserializationSchemaTest {
    @Test
    void preservesKafkaCoordinatesAndNullValues() throws Exception {
        var schema = new KafkaEnvelopeDeserializationSchema();
        List<KafkaRecord> output = new ArrayList<>();
        Collector<KafkaRecord> collector = collector(output);

        schema.deserialize(
                new ConsumerRecord<>(
                        "transactions.raw",
                        2,
                        42L,
                        "user-1".getBytes(StandardCharsets.UTF_8),
                        null),
                collector);

        assertEquals(1, output.size());
        KafkaRecord record = output.get(0);
        assertEquals("transactions.raw", record.getTopic());
        assertEquals(2, record.getPartition());
        assertEquals(42L, record.getOffset());
        assertEquals("user-1", record.getKey());
        assertNull(record.getValue());
    }

    private static Collector<KafkaRecord> collector(List<KafkaRecord> output) {
        return new Collector<>() {
            @Override
            public void collect(KafkaRecord record) {
                output.add(record);
            }

            @Override
            public void close() {
                // No resources are owned by this test collector.
            }
        };
    }
}
