package io.github.jaejungscene.realtimelab.job;

import io.github.jaejungscene.realtimelab.config.RiskRuleConfig;
import io.github.jaejungscene.realtimelab.model.AggregateEvent;
import io.github.jaejungscene.realtimelab.model.AlertEvent;
import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import io.github.jaejungscene.realtimelab.rule.RiskRules;
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

    @Test
    void incrementallyAggregatesWindowStateWithoutRetainingEveryEvent() {
        RiskRules rules = new RiskRules(RiskRuleConfig.defaults());
        RealTimeAlertJob.TransactionStatsAggregate aggregate =
                new RealTimeAlertJob.TransactionStatsAggregate(rules);
        RealTimeAlertJob.TransactionStats stats = aggregate.createAccumulator();

        stats = aggregate.add(transaction("event-b", 10.0, 0.2), stats);
        stats = aggregate.add(transaction("event-a", 25.0, 0.6), stats);

        assertEquals(2, stats.count());
        assertEquals(35.0, stats.totalAmount());
        assertEquals(0.4, stats.averageFraudScore(), 0.0001);
        assertEquals("event-a", stats.sampleEventId());
    }

    @Test
    void usesStableResultIdsAsKafkaKeys() {
        AlertEvent alert = AlertEvent.of(
                "USER_PAYMENT_BURST",
                "WARN",
                "user-1",
                "reason",
                60_000L,
                120_000L,
                120_000L,
                "eventCount",
                5.0,
                "event-1");
        AggregateEvent aggregate = new AggregateEvent();
        aggregate.setKey("KR|travel|merchant-1");
        aggregate.setWindowStart(60_000L);
        aggregate.setWindowEnd(120_000L);

        assertEquals(alert.getAlertId(), RealTimeAlertJob.alertKafkaKey(alert));
        assertEquals(
                "KR|travel|merchant-1|60000|120000",
                RealTimeAlertJob.aggregateKafkaKey(aggregate));
    }

    private static TransactionEvent transaction(String eventId, double amount, double fraudScore) {
        TransactionEvent event = new TransactionEvent();
        event.setEventId(eventId);
        event.setAmount(amount);
        event.setMlFraudScore(fraudScore);
        event.setMerchantRiskMultiplier(1.0);
        return event;
    }
}
