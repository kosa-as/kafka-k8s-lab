package com.example.kafka.producer;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TimestampProducer {
    private static final Logger log = LoggerFactory.getLogger(TimestampProducer.class);
    private TimestampProducer() {}
    public static void main(String[] args) throws InterruptedException {
        ProducerConfig config = ProducerConfig.fromSystemProperties(System.getProperties());
        log.info("Starting producer: topic={}, duration={}, frequencyPerSecond={}, bootstrapServers={}",
                config.topic(), config.duration(), config.frequencyPerSecond(), config.bootstrapServers());
        Properties kafka = new Properties();
        kafka.put("bootstrap.servers", config.bootstrapServers());
        kafka.put("key.serializer", StringSerializer.class.getName());
        kafka.put("value.serializer", StringSerializer.class.getName());
        kafka.put("acks", "all");
        kafka.put("enable.idempotence", "true");
        long deadline = System.nanoTime() + config.duration().toNanos();
        long intervalNanos = Math.max(1L, Duration.ofSeconds(1).toNanos() / config.frequencyPerSecond());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafka)) {
            long next = System.nanoTime();
            while (System.nanoTime() < deadline) {
                String key = UUID.randomUUID().toString();
                String value = Long.toString(System.currentTimeMillis());
                producer.send(new ProducerRecord<>(config.topic(), key, value), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send message: topic={}, key={}", config.topic(), key, exception);
                    } else {
                        log.info("Message sent: topic={}, partition={}, offset={}, key={}, value={}",
                                metadata.topic(), metadata.partition(), metadata.offset(), key, value);
                    }
                });
                next += intervalNanos;
                long wait = next - System.nanoTime();
                if (wait > 0) Thread.sleep(wait / 1_000_000L, (int) (wait % 1_000_000L));
            }
            producer.flush();
            log.info("Producer finished");
        }
    }
}
