package com.example.realtimelab.job;

import com.example.realtimelab.model.MerchantRiskProfile;
import com.example.realtimelab.serde.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MerchantRiskProfileParserTest {
    @Test
    void parsesDebeziumUnwrappedSnakeCasePayload() throws Exception {
        MerchantRiskProfile profile = MerchantRiskProfileParser.parse(
                """
                {
                  "merchant_id": "merchant-hot",
                  "risk_tier": "HIGH",
                  "risk_multiplier": 1.7,
                  "manual_review_required": true,
                  "updated_at": "2026-07-04T10:00:00Z"
                }
                """,
                ObjectMapperFactory.create());

        assertEquals("merchant-hot", profile.getMerchantId());
        assertEquals("HIGH", profile.getRiskTier());
        assertEquals(1.7, profile.getRiskMultiplier(), 0.0001);
        assertTrue(profile.isManualReviewRequired());
        assertEquals("2026-07-04T10:00:00Z", profile.getUpdatedAt());
    }

    @Test
    void parsesCamelCasePayloadForLocalTests() throws Exception {
        MerchantRiskProfile profile = MerchantRiskProfileParser.parse(
                """
                {
                  "merchantId": "merchant-01",
                  "riskTier": "MEDIUM",
                  "riskMultiplier": 1.2,
                  "manualReviewRequired": false
                }
                """,
                ObjectMapperFactory.create());

        assertEquals("merchant-01", profile.getMerchantId());
        assertEquals("MEDIUM", profile.getRiskTier());
        assertEquals(1.2, profile.getRiskMultiplier(), 0.0001);
    }

    @Test
    void rejectsPayloadWithoutMerchantId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MerchantRiskProfileParser.parse("{}", ObjectMapperFactory.create()));
    }
}
