package io.github.jaejungscene.realtimelab.rule;

import io.github.jaejungscene.realtimelab.config.RiskRuleConfig;
import io.github.jaejungscene.realtimelab.model.TransactionEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiskRulesTest {
    private final RiskRules rules = new RiskRules(RiskRuleConfig.defaults());

    @Test
    void highFraudScoreTriggersAlert() {
        TransactionEvent event = baseEvent();
        event.setMlFraudScore(0.95);

        assertTrue(rules.isHighRisk(event));
    }

    @Test
    void highAmountWithHighIpRiskTriggersAlert() {
        TransactionEvent event = baseEvent();
        event.setAmount(1_500.0);
        event.setIpRisk(90);

        assertTrue(rules.isHighRisk(event));
    }

    @Test
    void ordinaryPaymentDoesNotTriggerAlert() {
        TransactionEvent event = baseEvent();

        assertFalse(rules.isHighRisk(event));
    }

    @Test
    void merchantRiskMultiplierAdjustsFraudScore() {
        TransactionEvent event = baseEvent();
        event.setMlFraudScore(0.62);
        event.setMerchantRiskMultiplier(1.6);

        assertTrue(rules.isHighRisk(event));
        assertTrue(rules.effectiveFraudScore(event) <= 1.0);
    }

    @Test
    void manualReviewMerchantCanEscalateBorderlinePayment() {
        TransactionEvent event = baseEvent();
        event.setAmount(750.0);
        event.setMlFraudScore(0.76);
        event.setMerchantManualReviewRequired(true);

        assertTrue(rules.isHighRisk(event));
    }

    @Test
    void burstTriggersOnCountOrAmount() {
        assertTrue(rules.isBurst(5, 100.0));
        assertTrue(rules.isBurst(1, 3_000.0));
        assertFalse(rules.isBurst(4, 2_999.99));
    }

    @Test
    void merchantAnomalyTriggersOnCountAmountOrRiskConcentration() {
        assertTrue(rules.isMerchantAnomaly(25, 100.0, 0.1));
        assertTrue(rules.isMerchantAnomaly(2, 15_000.0, 0.1));
        assertTrue(rules.isMerchantAnomaly(5, 100.0, 0.72));
        assertFalse(rules.isMerchantAnomaly(4, 14_999.99, 0.71));
    }

    @Test
    void injectedThresholdsAreUsedInsteadOfTaskManagerEnvironment() {
        RiskRules strictRules = new RiskRules(new RiskRuleConfig(
                0.99,
                10_000.0,
                100,
                100,
                100_000.0,
                100,
                100_000.0,
                0.99));
        TransactionEvent event = baseEvent();
        event.setMlFraudScore(0.95);

        assertFalse(strictRules.isHighRisk(event));
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
