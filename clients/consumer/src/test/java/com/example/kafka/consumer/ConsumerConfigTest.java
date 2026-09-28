package com.example.kafka.consumer;

import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConsumerConfigTest {
    @Test void parsesDuration() {
        Properties p = new Properties();
        p.setProperty("duration", "15");
        assertEquals(Duration.ofSeconds(15), ConsumerConfig.fromSystemProperties(p).duration());
    }
    @Test void rejectsNonPositiveDuration() {
        Properties p = new Properties();
        p.setProperty("duration", "0");
        assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.fromSystemProperties(p));
    }

    @Test void rejectsMalformedDurationWithPropertyName() {
        Properties p = new Properties();
        p.setProperty("duration", "\\60");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ConsumerConfig.fromSystemProperties(p));
        assertTrue(error.getMessage().contains("duration"));
    }

    @Test void usesDefaultsWhenTopicAndGroupIdAreBlank() {
        Properties p = new Properties();
        p.setProperty("topic", "  ");
        p.setProperty("groupId", "");

        ConsumerConfig config = ConsumerConfig.fromSystemProperties(p);

        assertEquals("demo", config.topic());
        assertEquals("timestamp-consumer", config.groupId());
    }
}
