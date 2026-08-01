package io.github.jaejungscene.realtimelab.rule;

import io.github.jaejungscene.realtimelab.config.RiskRuleConfig;
import io.github.jaejungscene.realtimelab.model.TransactionEvent;

import java.io.Serializable;

public final class RiskRules implements Serializable {
    private final RiskRuleConfig config;

    public RiskRules(RiskRuleConfig config) {
        this.config = config;
    }

    public boolean isHighRisk(TransactionEvent event) {
        if (event == null) {
            return false;
        }

        double effectiveFraudScore = effectiveFraudScore(event);
        boolean modelSaysDanger = effectiveFraudScore >= config.highFraudScore();
        boolean expensiveRiskyPayment = event.getAmount() >= config.highAmount()
                && event.getIpRisk() >= config.highIpRisk();
        boolean suspiciousFailure = "FAILED".equalsIgnoreCase(event.getPaymentStatus())
                && effectiveFraudScore >= 0.85
                && event.getIpRisk() >= 70;
        boolean manualReviewRisk = event.isMerchantManualReviewRequired()
                && (effectiveFraudScore >= 0.75 || event.getAmount() >= config.highAmount() * 0.7);

        return modelSaysDanger || expensiveRiskyPayment || suspiciousFailure || manualReviewRisk;
    }

    public boolean isBurst(long eventCount, double totalAmount) {
        return eventCount >= config.burstCountThreshold()
                || totalAmount >= config.burstAmountThreshold();
    }

    public boolean isMerchantAnomaly(long eventCount, double totalAmount, double avgFraudScore) {
        boolean unusuallyBusy = eventCount >= config.merchantCountThreshold();
        boolean unusuallyExpensive = totalAmount >= config.merchantAmountThreshold();
        boolean consistentlyRisky = eventCount >= 5
                && avgFraudScore >= config.merchantAvgFraudScoreThreshold();
        return unusuallyBusy || unusuallyExpensive || consistentlyRisky;
    }

    public double effectiveFraudScore(TransactionEvent event) {
        if (event == null) {
            return 0.0;
        }
        double multiplier = event.getMerchantRiskMultiplier();
        return Math.min(1.0, event.getMlFraudScore() * multiplier);
    }

    public RiskRuleConfig config() {
        return config;
    }
}
