package com.example.kafka.consumer;

import java.time.Duration;
import java.util.Properties;

public record ConsumerConfig(Duration duration, String bootstrapServers, String topic, String groupId) {
    public static ConsumerConfig fromSystemProperties(Properties p) {
        String rawDuration = p.getProperty("duration", "60");
        final long seconds;
        try {
            seconds = Long.parseLong(rawDuration);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("duration must be a positive integer: " + rawDuration, e);
        }
        if (seconds <= 0) throw new IllegalArgumentException("duration must be positive");
        return new ConsumerConfig(Duration.ofSeconds(seconds), p.getProperty("bootstrapServers", "localhost:40000"),
                p.getProperty("topic", "demo"), p.getProperty("groupId", "timestamp-consumer"));
    }
}
