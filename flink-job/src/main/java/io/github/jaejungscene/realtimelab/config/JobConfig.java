package io.github.jaejungscene.realtimelab.config;

import org.apache.flink.connector.base.DeliveryGuarantee;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public record JobConfig(
        String bootstrapServers,
        String rawTopic,
        String replayTopic,
        String merchantRiskProfileTopic,
        String alertTopic,
        String aggregateTopic,
        String dlqTopic,
        String consumerGroup,
        DeliveryGuarantee sinkDeliveryGuarantee,
        long checkpointIntervalMillis,
        Duration watermarkDelay,
        Duration allowedLateness,
        Duration sourceIdleTimeout,
        Duration maxFutureSkew,
        SourceStartupMode sourceStartupMode,
        String transactionalIdPrefix) {

    private static final Set<String> SUPPORTED_ARGUMENTS = Set.of(
            "bootstrapServers",
            "rawTopic",
            "replayTopic",
            "merchantRiskProfileTopic",
            "alertTopic",
            "aggregateTopic",
            "dlqTopic",
            "consumerGroup",
            "sinkDeliveryGuarantee",
            "checkpointIntervalMillis",
            "watermarkDelaySeconds",
            "allowedLatenessSeconds",
            "sourceIdleTimeoutSeconds",
            "maxFutureSkewSeconds",
            "sourceStartupMode",
            "transactionalIdPrefix");

    public static JobConfig fromArgs(String[] args) {
        Map<String, String> params = parseArgs(args);
        JobConfig config = new JobConfig(
                value(params, "bootstrapServers", "kafka:9092"),
                value(params, "rawTopic", "transactions.raw"),
                value(params, "replayTopic", "transactions.replay"),
                value(params, "merchantRiskProfileTopic", "merchant_risk_profiles"),
                value(params, "alertTopic", "alerts.fraud"),
                value(params, "aggregateTopic", "transactions.aggregates"),
                value(params, "dlqTopic", "transactions.dlq"),
                value(params, "consumerGroup", "flink-realtime-lab"),
                deliveryGuarantee(params.getOrDefault("sinkDeliveryGuarantee", "AT_LEAST_ONCE")),
                positiveLong(params, "checkpointIntervalMillis", 10_000L),
                Duration.ofSeconds(nonNegativeLong(params, "watermarkDelaySeconds", 10L)),
                Duration.ofSeconds(nonNegativeLong(params, "allowedLatenessSeconds", 30L)),
                Duration.ofSeconds(positiveLong(params, "sourceIdleTimeoutSeconds", 30L)),
                Duration.ofSeconds(nonNegativeLong(params, "maxFutureSkewSeconds", 300L)),
                SourceStartupMode.parse(params.getOrDefault("sourceStartupMode", "LATEST")),
                value(params, "transactionalIdPrefix", "realtime-lab"));
        config.validate();
        return config;
    }

    private void validate() {
        if (rawTopic.equals(replayTopic)) {
            throw new IllegalArgumentException("rawTopic and replayTopic must be different");
        }
        if (new HashSet<>(List.of(
                        rawTopic,
                        replayTopic,
                        merchantRiskProfileTopic,
                        alertTopic,
                        aggregateTopic,
                        dlqTopic))
                .size() != 6) {
            throw new IllegalArgumentException("all configured Kafka topics must be distinct");
        }
        if (sinkDeliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE
                && (transactionalIdPrefix.length() < 3
                        || transactionalIdPrefix.chars().noneMatch(Character::isLetterOrDigit))) {
            throw new IllegalArgumentException(
                    "transactionalIdPrefix must contain at least 3 characters in EXACTLY_ONCE mode");
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            String arg = args[index];
            if (!arg.startsWith("--") || arg.length() == 2) {
                throw new IllegalArgumentException("expected --key value, got: " + arg);
            }
            String key = arg.substring(2);
            if (!SUPPORTED_ARGUMENTS.contains(key)) {
                throw new IllegalArgumentException("unsupported argument: --" + key);
            }
            if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
                throw new IllegalArgumentException("missing value for argument: --" + key);
            }
            if (params.putIfAbsent(key, args[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate argument: --" + key);
            }
        }
        return params;
    }

    private static String value(Map<String, String> params, String key, String fallback) {
        String result = params.getOrDefault(key, fallback);
        if (result == null || result.isBlank()) {
            throw new IllegalArgumentException(key + " must not be blank");
        }
        return result.trim();
    }

    private static long positiveLong(Map<String, String> params, String key, long fallback) {
        long value = longValue(params, key, fallback);
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be greater than 0: " + value);
        }
        return value;
    }

    private static long nonNegativeLong(Map<String, String> params, String key, long fallback) {
        long value = longValue(params, key, fallback);
        if (value < 0) {
            throw new IllegalArgumentException(key + " must not be negative: " + value);
        }
        return value;
    }

    private static long longValue(Map<String, String> params, String key, long fallback) {
        String rawValue = params.get(key);
        try {
            return rawValue == null || rawValue.isBlank() ? fallback : Long.parseLong(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer: " + rawValue, e);
        }
    }

    static DeliveryGuarantee deliveryGuarantee(String value) {
        try {
            DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.valueOf(value.trim().toUpperCase(Locale.ROOT));
            if (deliveryGuarantee != DeliveryGuarantee.AT_LEAST_ONCE
                    && deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE) {
                throw new IllegalArgumentException();
            }
            return deliveryGuarantee;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "sinkDeliveryGuarantee must be AT_LEAST_ONCE or EXACTLY_ONCE: " + value,
                    e);
        }
    }

    public enum SourceStartupMode {
        LATEST,
        EARLIEST;

        static SourceStartupMode parse(String value) {
            try {
                return SourceStartupMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "sourceStartupMode must be LATEST or EARLIEST: " + value,
                        e);
            }
        }
    }
}
