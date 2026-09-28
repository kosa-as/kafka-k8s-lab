package com.example.kafka.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TimestampConsumer {
    private static final Logger log = LoggerFactory.getLogger(TimestampConsumer.class);
    private TimestampConsumer() {}
    public static void main(String[] args) {
        ConsumerConfig config = ConsumerConfig.fromSystemProperties(System.getProperties());
        log.info("Starting consumer: topic={}, duration={}, groupId={}, bootstrapServers={}",
                config.topic(), config.duration(), config.groupId(), config.bootstrapServers());
        Properties kafka = new Properties();
        kafka.put("bootstrap.servers", config.bootstrapServers());
        kafka.put("group.id", config.groupId());
        kafka.put("key.deserializer", StringDeserializer.class.getName());
        kafka.put("value.deserializer", StringDeserializer.class.getName());
        kafka.put("auto.offset.reset", "earliest");
        kafka.put("enable.auto.commit", "true");
        long deadline = System.nanoTime() + config.duration().toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafka)) {
            consumer.subscribe(List.of(config.topic()));
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
                records.forEach(record -> log.info("Message received: topic={}, partition={}, offset={}, key={}, value={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));
            }
            log.info("Consumer finished");
        }
    }
}
