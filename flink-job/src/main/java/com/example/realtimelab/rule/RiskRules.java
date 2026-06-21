package com.example.realtimelab.rule;

import com.example.realtimelab.model.TransactionEvent;

public final class RiskRules {
    public static final double HIGH_FRAUD_SCORE = 0.92;
    public static final double HIGH_AMOUNT = 1_000.0;
    public static final int HIGH_IP_RISK = 80;
    public static final long BURST_COUNT_THRESHOLD = 5;
    public static final double BURST_AMOUNT_THRESHOLD = 3_000.0;
    public static final long MERCHANT_COUNT_THRESHOLD = 25;
    public static final double MERCHANT_AMOUNT_THRESHOLD = 15_000.0;
    public static final double MERCHANT_AVG_FRAUD_SCORE_THRESHOLD = 0.72;

    private RiskRules() {
    }

    public static boolean isHighRisk(TransactionEvent event) {
        if (event == null) {
            return false;
        }

        boolean modelSaysDanger = event.getMlFraudScore() >= HIGH_FRAUD_SCORE;
        boolean expensiveRiskyPayment = event.getAmount() >= HIGH_AMOUNT && event.getIpRisk() >= HIGH_IP_RISK;
        boolean suspiciousFailure = "FAILED".equalsIgnoreCase(event.getPaymentStatus())
                && event.getMlFraudScore() >= 0.85
                && event.getIpRisk() >= 70;

        return modelSaysDanger || expensiveRiskyPayment || suspiciousFailure;
    }

    public static boolean isBurst(long eventCount, double totalAmount) {
        return eventCount >= BURST_COUNT_THRESHOLD || totalAmount >= BURST_AMOUNT_THRESHOLD;
    }

    public static boolean isMerchantAnomaly(long eventCount, double totalAmount, double avgFraudScore) {
        boolean unusuallyBusy = eventCount >= MERCHANT_COUNT_THRESHOLD;
        boolean unusuallyExpensive = totalAmount >= MERCHANT_AMOUNT_THRESHOLD;
        boolean consistentlyRisky = eventCount >= 5 && avgFraudScore >= MERCHANT_AVG_FRAUD_SCORE_THRESHOLD;
        return unusuallyBusy || unusuallyExpensive || consistentlyRisky;
    }

    public static boolean isReplayCandidate(String errorType) {
        return "PARSE_OR_VALIDATION_ERROR".equals(errorType) || "LATE_EVENT".equals(errorType);
    }
}
