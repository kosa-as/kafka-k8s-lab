package com.example.kafka.producer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ValidationClient {
    private static final String MIN_ISR = "min.insync.replicas";
    private static final String TOPIC_PREFIX = "health-validation-";
    private static final Duration POLL = Duration.ofMillis(250);

    private ValidationClient() {}

    public static void main(String[] args) throws Exception {
        String command = args[0];
        String bootstrap = args[1];
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        if (command.equals("load")) {
            runLoad(bootstrap, topic, partition, Long.parseLong(args[4]), Boolean.parseBoolean(args[5]));
            return;
        }
        try (Admin admin = Admin.create(connection(bootstrap))) {
            switch (command) {
                case "create" -> {
                    requireExperimentTopic(topic);
                    int minIsr = Integer.parseInt(args[4]);
                    NewTopic experiment = args.length > 5
                            ? new NewTopic(topic, Map.of(0, List.of(Integer.parseInt(args[5]), 0, 1)))
                            : new NewTopic(topic, 1, (short) 3);
                    experiment.configs(Map.of(MIN_ISR, Integer.toString(minIsr), "retention.ms", "3600000"));
                    admin.createTopics(List.of(experiment)).all().get(6, TimeUnit.SECONDS);
                }
                case "describe" -> collectMetadata(admin, resource, partition);
                case "observe" -> {
                    AtomicBoolean running = stopOnInput();
                    try {
                        while (running.get()) {
                            long start = System.nanoTime();
                            try {
                                collectMetadata(admin, resource, partition);
                            } catch (Exception failure) {
                                event("metadata_error", failure.getClass().getSimpleName());
                            }
                            long remaining = TimeUnit.MILLISECONDS.toNanos(Long.parseLong(args[4]))
                                    - (System.nanoTime() - start);
                            if (remaining > 0) {
                                TimeUnit.NANOSECONDS.sleep(remaining);
                            }
                        }
                    } finally {
                        admin.close(Duration.ofSeconds(1));
                    }
                }
                default -> throw new IllegalArgumentException("Unknown command: " + command);
            }
        }
    }

    private static Properties connection(String bootstrap) {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("default.api.timeout.ms", "5000");
        config.put("request.timeout.ms", "2000");
        return config;
    }

    private static void requireExperimentTopic(String topic) {
        if (!topic.matches(TOPIC_PREFIX + "[a-z0-9-]+")) {
            throw new IllegalArgumentException("Topic changes require a unique experiment topic.");
        }
    }

    private static void collectMetadata(Admin admin, ConfigResource resource, int partition) throws Exception {
        long started = System.currentTimeMillis();
        var description = admin.describeTopics(List.of(resource.name())).topicNameValues().get(resource.name());
        var config = admin.describeConfigs(List.of(resource)).values().get(resource);
        var target = description.get(4, TimeUnit.SECONDS).partitions().stream()
                .filter(item -> item.partition() == partition).findFirst().orElseThrow();
        String minIsr = config.get(4, TimeUnit.SECONDS).get(MIN_ISR).value();
        String members = target.isr().stream().map(Node::idString).collect(Collectors.joining(","));
        event("partition", started, target.leader() == null ? -1 : target.leader().id(),
                members.isEmpty() ? "-" : members, minIsr, target.replicas().size());
    }

    private static void runLoad(String bootstrap, String topic, int partition, long intervalMs, boolean singleAttempt) throws Exception {
        if (intervalMs < 10 || intervalMs > 5000) {
            throw new IllegalArgumentException("Send interval must be in 10..5000 ms.");
        }
        AtomicBoolean running = stopOnInput();
        Thread consumer = new Thread(() -> consume(bootstrap, topic, partition, running), "validation-consumer");
        consumer.start();
        Properties config = connection(bootstrap);
        config.put("client.id", "health-validation-workload");
        config.put("acks", "all");
        config.put("enable.idempotence", Boolean.toString(!singleAttempt));
        config.put("retries", singleAttempt ? "0" : "1");
        config.put("delivery.timeout.ms", "4000");
        config.put("max.block.ms", "3000");
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        long interval = TimeUnit.MILLISECONDS.toNanos(intervalMs);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.partitionsFor(topic);
            event("producer_ready");
            while (running.get()) {
                long start = System.nanoTime();
                String id = UUID.randomUUID().toString();
                String sentAt = Long.toString(System.currentTimeMillis());
                event("send", id);
                try {
                    producer.send(new ProducerRecord<>(topic, partition, id, sentAt), (metadata, failure) -> {
                        long elapsedMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                        if (failure == null) {
                            event("ack", id, elapsedMicros, metadata.offset());
                        } else {
                            event("send_error", id, elapsedMicros, failure.getClass().getSimpleName());
                        }
                    });
                } catch (RuntimeException failure) {
                    event("send_error", id, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start),
                            failure.getClass().getSimpleName());
                }
                long remaining = interval - (System.nanoTime() - start);
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
            }
            producer.flush();
        } finally {
            running.set(false);
            consumer.join();
        }
    }

    private static void consume(String bootstrap, String topic, int partition, AtomicBoolean running) {
        Properties config = connection(bootstrap);
        config.put("enable.auto.commit", "false");
        config.put("key.deserializer", StringDeserializer.class.getName());
        config.put("value.deserializer", StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            TopicPartition target = new TopicPartition(topic, partition);
            consumer.assign(List.of(target));
            // The runner correlates deliveries by record ID, including on a reused read-only target.
            consumer.seekToEnd(List.of(target));
            consumer.position(target);
            event("consumer_ready");
            while (running.get()) {
                for (var record : consumer.poll(POLL)) {
                    event("consume", record.key(), record.offset());
                }
            }
        } catch (RuntimeException failure) {
            event("consumer_error", failure.getClass().getSimpleName());
        }
    }

    private static AtomicBoolean stopOnInput() {
        AtomicBoolean running = new AtomicBoolean(true);
        Thread control = new Thread(() -> {
            try {
                new BufferedReader(new InputStreamReader(System.in)).readLine();
            } catch (java.io.IOException failure) {
                event("control_error", failure.getClass().getSimpleName());
            } finally {
                running.set(false);
            }
        }, "validation-control");
        control.setDaemon(true);
        control.start();
        return running;
    }

    private static synchronized void event(String type, Object... fields) {
        StringBuilder line = new StringBuilder(Long.toString(System.currentTimeMillis())).append('\t').append(type);
        for (Object field : fields) {
            line.append('\t').append(field);
        }
        System.out.println(line);
    }
}
