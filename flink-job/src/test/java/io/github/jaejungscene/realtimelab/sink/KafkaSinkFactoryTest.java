package io.github.jaejungscene.realtimelab.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaSinkFactoryTest {
    @Test
    void sanitizesTransactionalIdComponents() {
        assertEquals(
                "realtime-lab-alerts-fraud-window-sink",
                KafkaSinkFactory.sanitize("realtime_lab.alerts.fraud/window sink"));
        assertEquals("scope", KafkaSinkFactory.sanitize("--scope--"));
    }
}
