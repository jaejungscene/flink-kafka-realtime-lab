package io.github.jaejungscene.realtimelab.job;

import io.github.jaejungscene.realtimelab.model.MerchantRiskProfile;
import io.github.jaejungscene.realtimelab.serde.ObjectMapperFactory;
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

    @Test
    void parsesDebeziumDeleteRewriteMarker() throws Exception {
        MerchantRiskProfile profile = MerchantRiskProfileParser.parse(
                """
                {
                  "merchant_id": "merchant-01",
                  "risk_tier": "LOW",
                  "risk_multiplier": 0.9,
                  "manual_review_required": false,
                  "__deleted": "true"
                }
                """,
                ObjectMapperFactory.create());

        assertEquals("merchant-01", profile.getMerchantId());
        assertTrue(profile.isDeleted());
    }

    @Test
    void rejectsInvalidProfileTypesAndRanges() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MerchantRiskProfileParser.parse(
                        "{\"merchant_id\":\"merchant-1\",\"risk_tier\":\"HIGH\",\"risk_multiplier\":\"bad\"}",
                        ObjectMapperFactory.create()));
        assertThrows(
                IllegalArgumentException.class,
                () -> MerchantRiskProfileParser.parse(
                        "{\"merchant_id\":\"merchant-1\",\"risk_tier\":\"UNKNOWN\",\"risk_multiplier\":1}",
                        ObjectMapperFactory.create()));
        assertThrows(
                IllegalArgumentException.class,
                () -> MerchantRiskProfileParser.parse(
                        "{\"merchant_id\":\"merchant-1\",\"risk_tier\":\"LOW\",\"risk_multiplier\":0}",
                        ObjectMapperFactory.create()));
    }

    @Test
    void canonicalFieldsTakePrecedenceOverAliases() throws Exception {
        MerchantRiskProfile profile = MerchantRiskProfileParser.parse(
                """
                {
                  "merchant_id": "merchant-1",
                  "merchantId": 123,
                  "risk_tier": "LOW",
                  "riskTier": false,
                  "risk_multiplier": 1
                }
                """,
                ObjectMapperFactory.create());

        assertEquals("merchant-1", profile.getMerchantId());
        assertEquals("LOW", profile.getRiskTier());
    }

    @Test
    void rejectsNonTextualMerchantIdentifiers() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MerchantRiskProfileParser.parse(
                        "{\"merchant_id\":123,\"risk_tier\":\"LOW\"}",
                        ObjectMapperFactory.create()));
    }
}
