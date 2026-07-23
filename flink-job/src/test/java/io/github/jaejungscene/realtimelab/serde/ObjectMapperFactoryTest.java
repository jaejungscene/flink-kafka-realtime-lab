package io.github.jaejungscene.realtimelab.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObjectMapperFactoryTest {
    @Test
    void ignoresUnknownFieldsForForwardCompatibleEvents() throws Exception {
        ObjectMapper mapper = ObjectMapperFactory.create();

        TransactionEvent event = mapper.readValue("""
                {
                  "eventId": "evt-1",
                  "userId": "user-1",
                  "eventTime": 1760000000000,
                  "amount": 42.5,
                  "mlFraudScore": 0.13,
                  "unexpectedNewField": "safe to ignore"
                }
                """, TransactionEvent.class);

        assertEquals("evt-1", event.getEventId());
        assertEquals("user-1", event.getUserId());
        assertEquals(42.5, event.getAmount());
    }

    @Test
    void rejectsTrailingJsonValues() {
        ObjectMapper mapper = ObjectMapperFactory.create();

        assertThrows(
                JsonProcessingException.class,
                () -> mapper.readValue(
                        "{\"eventId\":\"evt-1\",\"userId\":\"user-1\"} {\"extra\":true}",
                        TransactionEvent.class));
    }
}
