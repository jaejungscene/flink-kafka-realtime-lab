package io.github.jaejungscene.realtimelab.config;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobConfigTest {
    @Test
    void acceptsSupportedDeliveryGuarantees() {
        assertEquals(
                DeliveryGuarantee.AT_LEAST_ONCE,
                JobConfig.fromArgs(new String[]{"--sinkDeliveryGuarantee", "AT_LEAST_ONCE"})
                        .sinkDeliveryGuarantee());
        assertEquals(
                DeliveryGuarantee.EXACTLY_ONCE,
                JobConfig.fromArgs(new String[]{
                                "--sinkDeliveryGuarantee", "exactly_once",
                                "--transactionalIdPrefix", "test-job"})
                        .sinkDeliveryGuarantee());
    }

    @Test
    void rejectsUnsupportedDeliveryGuarantee() {
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--sinkDeliveryGuarantee", "NONE"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--sinkDeliveryGuarantee", "sometimes"}));
    }

    @Test
    void rejectsUnknownDuplicateAndMissingArguments() {
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--unknown", "value"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--rawTopic", "one", "--rawTopic", "two"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--rawTopic"}));
    }

    @Test
    void rejectsUnsafeTimingAndTopicConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--checkpointIntervalMillis", "0"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{
                        "--rawTopic", "same",
                        "--replayTopic", "same"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{
                        "--alertTopic", "same",
                        "--aggregateTopic", "same"}));
        assertThrows(
                IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{
                        "--sinkDeliveryGuarantee", "EXACTLY_ONCE",
                        "--transactionalIdPrefix", "---"}));
    }
}
