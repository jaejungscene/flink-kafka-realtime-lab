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

    @Test
    void keepsWindowAlertIdStableWhenItsSampleChanges() {
        AlertEvent first = windowAlert("event-1");
        AlertEvent lateUpdate = windowAlert("event-0");

        assertEquals(first.getAlertId(), lateUpdate.getAlertId());
        assertNotEquals(first.getSampleEventId(), lateUpdate.getSampleEventId());
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

    private static AlertEvent windowAlert(String sampleEventId) {
        return AlertEvent.of(
                "USER_PAYMENT_BURST",
                "WARN",
                "user-1",
                "reason",
                60_000L,
                120_000L,
                120_000L,
                "eventCount",
                5.0,
                sampleEventId);
    }
}
