package io.github.jaejungscene.realtimelab.sink;

import io.github.jaejungscene.realtimelab.serde.JsonSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public final class KafkaSinkFactory {
    private KafkaSinkFactory() {
    }

    public static <T> KafkaSink<T> create(
            String bootstrapServers,
            String topic,
            KeyExtractor<T> keyExtractor,
            DeliveryGuarantee deliveryGuarantee,
            String transactionalIdPrefix,
            String transactionalScope) {
        var builder = KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(deliveryGuarantee)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(new StringKeySerializationSchema<>(keyExtractor))
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build());

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            String prefix = sanitize(transactionalIdPrefix + "-" + topic + "-" + transactionalScope);
            builder.setTransactionalIdPrefix(prefix + "-");
        }
        return builder.build();
    }

    static String sanitize(String value) {
        String sanitized = value.replaceAll("[^A-Za-z0-9-]", "-").replaceAll("-+", "-");
        return sanitized.replaceAll("^-|-$", "");
    }

    @FunctionalInterface
    public interface KeyExtractor<T> extends Serializable {
        String key(T element);
    }

    private static final class StringKeySerializationSchema<T> implements SerializationSchema<T> {
        private final KeyExtractor<T> keyExtractor;

        private StringKeySerializationSchema(KeyExtractor<T> keyExtractor) {
            this.keyExtractor = keyExtractor;
        }

        @Override
        public byte[] serialize(T element) {
            String key = keyExtractor.key(element);
            return key == null || key.isBlank() ? null : key.getBytes(StandardCharsets.UTF_8);
        }
    }
}
