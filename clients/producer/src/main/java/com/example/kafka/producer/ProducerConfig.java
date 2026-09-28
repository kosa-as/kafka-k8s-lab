package com.example.kafka.producer;

import java.time.Duration;
import java.util.Properties;

public record ProducerConfig(Duration duration, int frequencyPerSecond, String bootstrapServers, String topic) {
    public static ProducerConfig fromSystemProperties(Properties p) {
        long seconds = positiveLong(p, "duration", 60);
        int frequency = positiveInt(p, "frequency", 1);
        return new ProducerConfig(Duration.ofSeconds(seconds), frequency,
                p.getProperty("bootstrapServers", "localhost:40000"), p.getProperty("topic", "demo"));
    }
    private static long positiveLong(Properties p, String key, long fallback) {
        String raw = p.getProperty(key, Long.toString(fallback));
        final long value;
        try {
            value = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a positive integer: " + raw, e);
        }
        if (value <= 0) throw new IllegalArgumentException(key + " must be positive");
        return value;
    }
    private static int positiveInt(Properties p, String key, int fallback) {
        String raw = p.getProperty(key, Integer.toString(fallback));
        final int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a positive integer: " + raw, e);
        }
        if (value <= 0) throw new IllegalArgumentException(key + " must be positive");
        return value;
    }
}
