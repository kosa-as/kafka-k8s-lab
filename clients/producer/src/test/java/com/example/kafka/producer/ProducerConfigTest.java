package com.example.kafka.producer;

import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProducerConfigTest {
    @Test void parsesDurationAndFrequency() {
        Properties p = new Properties();
        p.setProperty("duration", "12");
        p.setProperty("frequency", "4");
        ProducerConfig config = ProducerConfig.fromSystemProperties(p);
        assertEquals(Duration.ofSeconds(12), config.duration());
        assertEquals(4, config.frequencyPerSecond());
    }
    @Test void rejectsNonPositiveFrequency() {
        Properties p = new Properties();
        p.setProperty("frequency", "0");
        assertThrows(IllegalArgumentException.class, () -> ProducerConfig.fromSystemProperties(p));
    }

    @Test void rejectsMalformedDurationWithPropertyName() {
        Properties p = new Properties();
        p.setProperty("duration", "\\60");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> ProducerConfig.fromSystemProperties(p));
        assertTrue(error.getMessage().contains("duration"));
    }
}
