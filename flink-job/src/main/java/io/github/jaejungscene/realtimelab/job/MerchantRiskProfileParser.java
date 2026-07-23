package io.github.jaejungscene.realtimelab.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jaejungscene.realtimelab.model.DlqEvent;
import io.github.jaejungscene.realtimelab.model.KafkaRecord;
import io.github.jaejungscene.realtimelab.model.MerchantRiskProfile;
import io.github.jaejungscene.realtimelab.serde.ObjectMapperFactory;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Locale;
import java.util.Set;

public class MerchantRiskProfileParser extends ProcessFunction<KafkaRecord, MerchantRiskProfile> {
    private final OutputTag<DlqEvent> dlqTag;
    private transient ObjectMapper mapper;

    public MerchantRiskProfileParser(OutputTag<DlqEvent> dlqTag) {
        this.dlqTag = dlqTag;
    }

    @Override
    public void processElement(KafkaRecord record, Context ctx, Collector<MerchantRiskProfile> out) {
        try {
            out.collect(parse(record.getValue(), mapper()));
        } catch (Exception e) {
            ctx.output(dlqTag, new DlqEvent(
                    "REFERENCE_DATA_PARSE_ERROR",
                    errorMessage(e),
                    record.getTopic(),
                    record.getPartition(),
                    record.getOffset(),
                    record.getTimestamp(),
                    record.getKey(),
                    null,
                    record.getValue(),
                    System.currentTimeMillis()));
        }
    }

    static MerchantRiskProfile parse(String rawValue, ObjectMapper mapper) throws Exception {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("record value must not be null or blank");
        }
        JsonNode root = mapper.readTree(rawValue);
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("merchant profile payload must be a JSON object");
        }
        String merchantId = text(root, "merchant_id", text(root, "merchantId", null));
        if (merchantId == null || merchantId.isBlank()) {
            throw new IllegalArgumentException("merchant_id is required");
        }

        MerchantRiskProfile profile = new MerchantRiskProfile();
        profile.setMerchantId(merchantId);
        profile.setDeleted(debeziumBoolean(root, "__deleted", false));
        if (profile.isDeleted()) {
            return profile;
        }

        String riskTier = text(root, "risk_tier", text(root, "riskTier", "UNKNOWN")).toUpperCase(Locale.ROOT);
        if (!Set.of("LOW", "MEDIUM", "HIGH").contains(riskTier)) {
            throw new IllegalArgumentException("risk_tier must be LOW, MEDIUM, or HIGH");
        }
        profile.setRiskTier(riskTier);
        double riskMultiplier = requiredDouble(root, "risk_multiplier", "riskMultiplier", 1.0);
        if (!Double.isFinite(riskMultiplier) || riskMultiplier <= 0 || riskMultiplier > 10) {
            throw new IllegalArgumentException("risk_multiplier must be greater than 0 and at most 10");
        }
        profile.setRiskMultiplier(riskMultiplier);
        profile.setManualReviewRequired(requiredBoolean(
                root,
                "manual_review_required",
                "manualReviewRequired",
                false));
        profile.setUpdatedAt(text(root, "updated_at", text(root, "updatedAt", null)));
        return profile;
    }

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = ObjectMapperFactory.create();
        }
        return mapper;
    }

    private static String text(JsonNode root, String fieldName, String fallback) {
        JsonNode value = root.get(fieldName);
        if (value == null || value.isNull()) {
            return fallback;
        }
        return value.asText();
    }

    private static double requiredDouble(JsonNode root, String snakeCase, String camelCase, double fallback) {
        JsonNode value = field(root, snakeCase, camelCase);
        if (value == null || value.isNull()) {
            return fallback;
        }
        if (!value.isNumber()) {
            throw new IllegalArgumentException(snakeCase + " must be numeric");
        }
        return value.doubleValue();
    }

    private static boolean requiredBoolean(JsonNode root, String snakeCase, String camelCase, boolean fallback) {
        JsonNode value = field(root, snakeCase, camelCase);
        if (value == null || value.isNull()) {
            return fallback;
        }
        if (!value.isBoolean()) {
            throw new IllegalArgumentException(snakeCase + " must be boolean");
        }
        return value.booleanValue();
    }

    private static boolean debeziumBoolean(JsonNode root, String fieldName, boolean fallback) {
        JsonNode value = root.get(fieldName);
        if (value == null || value.isNull()) {
            return fallback;
        }
        if (value.isBoolean()) {
            return value.booleanValue();
        }
        if (value.isTextual() && ("true".equalsIgnoreCase(value.textValue())
                || "false".equalsIgnoreCase(value.textValue()))) {
            return Boolean.parseBoolean(value.textValue());
        }
        throw new IllegalArgumentException(fieldName + " must be boolean");
    }

    private static JsonNode field(JsonNode root, String snakeCase, String camelCase) {
        JsonNode value = root.get(snakeCase);
        return value == null ? root.get(camelCase) : value;
    }

    private static String errorMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
    }
}
