package io.github.jaejungscene.realtimelab.config;

import java.io.Serializable;

public record RiskRuleConfig(
        double highFraudScore,
        double highAmount,
        int highIpRisk,
        long burstCountThreshold,
        double burstAmountThreshold,
        long merchantCountThreshold,
        double merchantAmountThreshold,
        double merchantAvgFraudScoreThreshold) implements Serializable {

    public RiskRuleConfig {
        requireBounded(highFraudScore, 0.0, 1.0, "highFraudScore");
        requireBounded(highAmount, 0.0, Double.MAX_VALUE, "highAmount");
        if (highIpRisk < 0 || highIpRisk > 100) {
            throw new IllegalArgumentException("highIpRisk must be between 0 and 100");
        }
        if (burstCountThreshold <= 0) {
            throw new IllegalArgumentException("burstCountThreshold must be greater than 0");
        }
        requireBounded(burstAmountThreshold, 0.0, Double.MAX_VALUE, "burstAmountThreshold");
        if (merchantCountThreshold <= 0) {
            throw new IllegalArgumentException("merchantCountThreshold must be greater than 0");
        }
        requireBounded(merchantAmountThreshold, 0.0, Double.MAX_VALUE, "merchantAmountThreshold");
        requireBounded(
                merchantAvgFraudScoreThreshold,
                0.0,
                1.0,
                "merchantAvgFraudScoreThreshold");
    }

    public static RiskRuleConfig defaults() {
        return new RiskRuleConfig(
                0.92,
                1_000.0,
                80,
                5,
                3_000.0,
                25,
                15_000.0,
                0.72);
    }

    private static void requireBounded(double value, double minimum, double maximum, String name) {
        if (!Double.isFinite(value) || value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    name + " must be between " + minimum + " and " + maximum);
        }
    }
}
