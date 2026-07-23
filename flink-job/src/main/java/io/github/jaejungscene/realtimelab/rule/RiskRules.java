package io.github.jaejungscene.realtimelab.rule;

import io.github.jaejungscene.realtimelab.model.TransactionEvent;

public final class RiskRules {
    public static final double HIGH_FRAUD_SCORE = boundedDoubleSetting("RISK_HIGH_FRAUD_SCORE", 0.92, 0.0, 1.0);
    public static final double HIGH_AMOUNT = boundedDoubleSetting(
            "RISK_HIGH_AMOUNT", 1_000.0, 0.0, Double.MAX_VALUE);
    public static final int HIGH_IP_RISK = boundedIntSetting("RISK_HIGH_IP_RISK", 80, 0, 100);
    public static final long BURST_COUNT_THRESHOLD = positiveLongSetting("RISK_BURST_COUNT_THRESHOLD", 5);
    public static final double BURST_AMOUNT_THRESHOLD = boundedDoubleSetting(
            "RISK_BURST_AMOUNT_THRESHOLD", 3_000.0, 0.0, Double.MAX_VALUE);
    public static final long MERCHANT_COUNT_THRESHOLD = positiveLongSetting("RISK_MERCHANT_COUNT_THRESHOLD", 25);
    public static final double MERCHANT_AMOUNT_THRESHOLD = boundedDoubleSetting(
            "RISK_MERCHANT_AMOUNT_THRESHOLD", 15_000.0, 0.0, Double.MAX_VALUE);
    public static final double MERCHANT_AVG_FRAUD_SCORE_THRESHOLD =
            boundedDoubleSetting("RISK_MERCHANT_AVG_FRAUD_SCORE_THRESHOLD", 0.72, 0.0, 1.0);

    private RiskRules() {
    }

    public static boolean isHighRisk(TransactionEvent event) {
        if (event == null) {
            return false;
        }

        double effectiveFraudScore = effectiveFraudScore(event);
        boolean modelSaysDanger = effectiveFraudScore >= HIGH_FRAUD_SCORE;
        boolean expensiveRiskyPayment = event.getAmount() >= HIGH_AMOUNT && event.getIpRisk() >= HIGH_IP_RISK;
        boolean suspiciousFailure = "FAILED".equalsIgnoreCase(event.getPaymentStatus())
                && effectiveFraudScore >= 0.85
                && event.getIpRisk() >= 70;
        boolean manualReviewRisk = event.isMerchantManualReviewRequired()
                && (effectiveFraudScore >= 0.75 || event.getAmount() >= HIGH_AMOUNT * 0.7);

        return modelSaysDanger || expensiveRiskyPayment || suspiciousFailure || manualReviewRisk;
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
        return "PARSE_OR_VALIDATION_ERROR".equals(errorType);
    }

    public static double effectiveFraudScore(TransactionEvent event) {
        if (event == null) {
            return 0.0;
        }
        double multiplier = event.getMerchantRiskMultiplier();
        return Math.min(1.0, event.getMlFraudScore() * multiplier);
    }

    private static double boundedDoubleSetting(String key, double fallback, double minimum, double maximum) {
        String rawValue = System.getenv(key);
        double value;
        try {
            value = rawValue == null || rawValue.isBlank() ? fallback : Double.parseDouble(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a number: " + rawValue, e);
        }
        if (!Double.isFinite(value) || value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    key + " must be between " + minimum + " and " + maximum + ": " + value);
        }
        return value;
    }

    private static int boundedIntSetting(String key, int fallback, int minimum, int maximum) {
        String rawValue = System.getenv(key);
        int value;
        try {
            value = rawValue == null || rawValue.isBlank() ? fallback : Integer.parseInt(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer: " + rawValue, e);
        }
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    key + " must be between " + minimum + " and " + maximum + ": " + value);
        }
        return value;
    }

    private static long positiveLongSetting(String key, long fallback) {
        String rawValue = System.getenv(key);
        long value;
        try {
            value = rawValue == null || rawValue.isBlank() ? fallback : Long.parseLong(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer: " + rawValue, e);
        }
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be greater than 0: " + value);
        }
        return value;
    }
}
