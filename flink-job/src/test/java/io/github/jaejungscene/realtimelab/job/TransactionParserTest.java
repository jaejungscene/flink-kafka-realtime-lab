package io.github.jaejungscene.realtimelab.job;

import io.github.jaejungscene.realtimelab.model.KafkaRecord;
import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import io.github.jaejungscene.realtimelab.serde.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionParserTest {
    @Test
    void preservesKafkaAndReplayLineage() throws Exception {
        KafkaRecord record = new KafkaRecord(
                "transactions.replay",
                2,
                42L,
                1_760_000_000_000L,
                "user-1",
                """
                {
                  "eventId": "event-1",
                  "userId": "user-1",
                  "eventTime": 1760000000000,
                  "amount": 10.5,
                  "replayId": "run-1-0-7",
                  "replayRunId": "run-1",
                  "replaySourceTopic": "transactions.dlq",
                  "replaySourcePartition": 0,
                  "replaySourceOffset": 7,
                  "replayedFromDlqAt": 1760000001000
                }
                """);

        TransactionEvent event = TransactionParser.parse(record, ObjectMapperFactory.create());

        assertEquals("transactions.replay", event.getSourceTopic());
        assertEquals(2, event.getSourcePartition());
        assertEquals(42L, event.getSourceOffset());
        assertEquals("run-1-0-7", event.getReplayId());
        assertEquals(7L, event.getReplaySourceOffset());
        assertEquals(record.getValue(), event.getOriginalRawValue());
    }

    @Test
    void rejectsNullKafkaValues() {
        KafkaRecord record = new KafkaRecord("transactions.raw", 0, 1L, 1L, null, null);

        assertThrows(
                IllegalArgumentException.class,
                () -> TransactionParser.parse(record, ObjectMapperFactory.create()));
    }

    @Test
    void rejectsFutureAndOutOfRangeRiskValues() {
        KafkaRecord future = recordWith(2_000L, 0.5, 10);
        KafkaRecord invalidScore = recordWith(1_000L, 1.1, 10);
        KafkaRecord invalidIpRisk = recordWith(1_000L, 0.5, 101);

        assertThrows(
                IllegalArgumentException.class,
                () -> TransactionParser.parse(future, ObjectMapperFactory.create(), 1_000L, 100L));
        assertThrows(
                IllegalArgumentException.class,
                () -> TransactionParser.parse(invalidScore, ObjectMapperFactory.create(), 1_000L, 100L));
        assertThrows(
                IllegalArgumentException.class,
                () -> TransactionParser.parse(invalidIpRisk, ObjectMapperFactory.create(), 1_000L, 100L));
    }

    private static KafkaRecord recordWith(long eventTime, double score, int ipRisk) {
        String value = "{\"eventId\":\"event-1\",\"userId\":\"user-1\",\"eventTime\":"
                + eventTime
                + ",\"amount\":10,\"mlFraudScore\":"
                + score
                + ",\"ipRisk\":"
                + ipRisk
                + "}";
        return new KafkaRecord("transactions.raw", 0, 1L, 1L, "user-1", value);
    }
}
