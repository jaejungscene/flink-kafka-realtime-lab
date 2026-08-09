package io.github.jaejungscene.realtimelab.job;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventDeduplicatorTest {
    @Test
    void requiresAPositiveStateTtl() {
        assertDoesNotThrow(() -> new EventDeduplicator(Duration.ofHours(24)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new EventDeduplicator(Duration.ZERO));
        assertThrows(
                IllegalArgumentException.class,
                () -> new EventDeduplicator(Duration.ofSeconds(-1)));
    }
}
