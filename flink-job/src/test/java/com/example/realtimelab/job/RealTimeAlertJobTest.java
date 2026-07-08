package com.example.realtimelab.job;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RealTimeAlertJobTest {
    @Test
    void acceptsSupportedDeliveryGuarantees() {
        assertEquals(
                DeliveryGuarantee.AT_LEAST_ONCE,
                RealTimeAlertJob.deliveryGuaranteeParam("AT_LEAST_ONCE"));
        assertEquals(
                DeliveryGuarantee.EXACTLY_ONCE,
                RealTimeAlertJob.deliveryGuaranteeParam("exactly_once"));
    }

    @Test
    void rejectsUnsupportedDeliveryGuarantee() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RealTimeAlertJob.deliveryGuaranteeParam("NONE"));
        assertThrows(
                IllegalArgumentException.class,
                () -> RealTimeAlertJob.deliveryGuaranteeParam("sometimes"));
    }
}
