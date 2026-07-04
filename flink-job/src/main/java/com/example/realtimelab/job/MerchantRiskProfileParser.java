package com.example.realtimelab.job;

import com.example.realtimelab.model.DlqEvent;
import com.example.realtimelab.model.MerchantRiskProfile;
import com.example.realtimelab.serde.ObjectMapperFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MerchantRiskProfileParser extends ProcessFunction<String, MerchantRiskProfile> {
    private final OutputTag<DlqEvent> dlqTag;
    private final String sourceTopic;
    private transient ObjectMapper mapper;

    public MerchantRiskProfileParser(OutputTag<DlqEvent> dlqTag, String sourceTopic) {
        this.dlqTag = dlqTag;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void processElement(String rawValue, Context ctx, Collector<MerchantRiskProfile> out) {
        try {
            out.collect(parse(rawValue, mapper()));
        } catch (Exception e) {
            ctx.output(dlqTag, new DlqEvent(
                    "REFERENCE_DATA_PARSE_ERROR",
                    e.getMessage(),
                    sourceTopic,
                    null,
                    rawValue,
                    System.currentTimeMillis()));
        }
    }

    static MerchantRiskProfile parse(String rawValue, ObjectMapper mapper) throws Exception {
        JsonNode root = mapper.readTree(rawValue);
        String merchantId = text(root, "merchant_id", text(root, "merchantId", null));
        if (merchantId == null || merchantId.isBlank()) {
            throw new IllegalArgumentException("merchant_id is required");
        }

        MerchantRiskProfile profile = new MerchantRiskProfile();
        profile.setMerchantId(merchantId);
        profile.setRiskTier(text(root, "risk_tier", text(root, "riskTier", "UNKNOWN")));
        profile.setRiskMultiplier(doubleValue(root, "risk_multiplier", doubleValue(root, "riskMultiplier", 1.0)));
        profile.setManualReviewRequired(booleanValue(
                root,
                "manual_review_required",
                booleanValue(root, "manualReviewRequired", false)));
        profile.setUpdatedAt(text(root, "updated_at", text(root, "updatedAt", null)));
        profile.setDeleted(booleanValue(root, "__deleted", false));
        if (profile.getRiskMultiplier() <= 0) {
            profile.setRiskMultiplier(1.0);
        }
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

    private static double doubleValue(JsonNode root, String fieldName, double fallback) {
        JsonNode value = root.get(fieldName);
        if (value == null || value.isNull()) {
            return fallback;
        }
        return value.asDouble(fallback);
    }

    private static boolean booleanValue(JsonNode root, String fieldName, boolean fallback) {
        JsonNode value = root.get(fieldName);
        if (value == null || value.isNull()) {
            return fallback;
        }
        return value.asBoolean(fallback);
    }
}
