package com.example.realtimelab.rule;

import com.example.realtimelab.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiskRulesTest {
    @Test
    void highFraudScoreTriggersAlert() {
        TransactionEvent event = baseEvent();
        event.setMlFraudScore(0.95);

        assertTrue(RiskRules.isHighRisk(event));
    }

    @Test
    void highAmountWithHighIpRiskTriggersAlert() {
        TransactionEvent event = baseEvent();
        event.setAmount(1_500.0);
        event.setIpRisk(90);

        assertTrue(RiskRules.isHighRisk(event));
    }

    @Test
    void ordinaryPaymentDoesNotTriggerAlert() {
        TransactionEvent event = baseEvent();

        assertFalse(RiskRules.isHighRisk(event));
    }

    @Test
    void burstTriggersOnCountOrAmount() {
        assertTrue(RiskRules.isBurst(5, 100.0));
        assertTrue(RiskRules.isBurst(1, 3_000.0));
        assertFalse(RiskRules.isBurst(4, 2_999.99));
    }

    @Test
    void merchantAnomalyTriggersOnCountAmountOrRiskConcentration() {
        assertTrue(RiskRules.isMerchantAnomaly(25, 100.0, 0.1));
        assertTrue(RiskRules.isMerchantAnomaly(2, 15_000.0, 0.1));
        assertTrue(RiskRules.isMerchantAnomaly(5, 100.0, 0.72));
        assertFalse(RiskRules.isMerchantAnomaly(4, 14_999.99, 0.71));
    }

    @Test
    void replayCandidateIsLimitedToRecoverableDlqTypes() {
        assertTrue(RiskRules.isReplayCandidate("PARSE_OR_VALIDATION_ERROR"));
        assertTrue(RiskRules.isReplayCandidate("LATE_EVENT"));
        assertFalse(RiskRules.isReplayCandidate("POISON_PILL"));
    }

    private static TransactionEvent baseEvent() {
        TransactionEvent event = new TransactionEvent();
        event.setEventId("evt-1");
        event.setUserId("user-1");
        event.setEventTime(System.currentTimeMillis());
        event.setAmount(20.0);
        event.setMlFraudScore(0.2);
        event.setIpRisk(10);
        event.setPaymentStatus("APPROVED");
        return event;
    }
}
