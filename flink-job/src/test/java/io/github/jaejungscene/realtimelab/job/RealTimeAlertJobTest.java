package io.github.jaejungscene.realtimelab.job;

import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealTimeAlertJobTest {
    @Test
    void keepsEventsUntilTheWindowCleanupDeadline() {
        long eventTime = 65_000L;
        long windowSize = 60_000L;
        long allowedLateness = 30_000L;

        assertFalse(RealTimeAlertJob.isPastWindowCleanup(
                eventTime,
                100_000L,
                windowSize,
                allowedLateness));
        assertFalse(RealTimeAlertJob.isPastWindowCleanup(
                eventTime,
                149_998L,
                windowSize,
                allowedLateness));
        assertTrue(RealTimeAlertJob.isPastWindowCleanup(
                eventTime,
                149_999L,
                windowSize,
                allowedLateness));
    }

    @Test
    void normalizesAggregateDimensionsAndUsesExplicitFallbacks() {
        TransactionEvent event = new TransactionEvent();
        event.setCountry(" KR ");
        event.setCategory("  ");
        event.setMerchantId(null);

        assertEquals(
                "KR|uncategorized|merchant-unknown",
                RealTimeAlertJob.aggregateKey(event));
    }
}
