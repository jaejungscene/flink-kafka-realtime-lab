package com.example.realtimelab.serde;

import com.example.realtimelab.model.TransactionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
