package io.github.jaejungscene.realtimelab.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class AlertEventTest {
    @Test
    void createsStableIdsForRetriesOfTheSameAlert() {
        AlertEvent first = alert("event-1");
        AlertEvent retry = alert("event-1");
        AlertEvent anotherEvent = alert("event-2");

        assertEquals(first.getAlertId(), retry.getAlertId());
        assertNotEquals(first.getAlertId(), anotherEvent.getAlertId());
    }

    private static AlertEvent alert(String sampleEventId) {
        return AlertEvent.of(
                "HIGH_RISK_TRANSACTION",
                "CRITICAL",
                "user-1",
                "reason",
                100L,
                100L,
                100L,
                "effectiveFraudScore",
                0.95,
                sampleEventId);
    }
}
