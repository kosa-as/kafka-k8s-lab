# Kafka Producer and Consumer Implementation Plan

> For agentic workers: use superpowers executing-plans task-by-task. Steps use checkboxes for tracking.

**Goal:** Build Java/Maven producer and consumer command-line applications for the workspace Strimzi Kafka cluster.

**Architecture:** Producer and consumer are sibling Maven projects. Each uses Apache Kafka client, exposes runtime properties via Maven, and relies on the preconfigured demo topic. Configuration parsing is separated from Kafka loops so it can be unit-tested without a broker.

**Tech Stack:** Java 17+, Maven 3.9+, Apache Kafka Clients 4.3.1, JUnit 5, Maven Surefire and Exec plugins.

**Spec:** .planning/2026-09-22-kafka-producer-consumer/task_plan.md

## Global Constraints

- Preserve the existing Strimzi configuration and topic manifest.
- Producer properties: duration seconds and frequency messages per second.
- Consumer property: duration seconds.
- Payload: current epoch timestamp in milliseconds.
- Bootstrap address configurable by Maven property bootstrapServers.

### Task 1: Establish Runtime Defaults

**Files:** root README, existing Strimzi manifests.

- [ ] Query NodePort using kubectl get svc kafka-kafka-external-bootstrap -n kafka.
- [ ] Document the topic demo and the internal bootstrap endpoint.

### Task 2: Build Producer with TDD

**Files:** clients/pom.xml; producer/pom.xml; ProducerConfig.java; TimestampProducer.java; ProducerConfigTest.java; producer/README.md.

- [ ] Write a failing test that duration=12 and frequency=4 parse into a 12-second duration and frequency 4.
- [ ] Run mvn -q -f clients/producer/pom.xml test and observe the missing-class failure.
- [ ] Implement property parsing and a KafkaProducer loop using StringSerializer, acks=all, idempotence, random record key, and a timestamp payload.
- [ ] Run mvn -q -f clients/producer/pom.xml test package.

### Task 3: Build Consumer with TDD

**Files:** clients/pom.xml; consumer/pom.xml; ConsumerConfig.java; TimestampConsumer.java; ConsumerConfigTest.java; consumer/README.md.

- [ ] Write a failing test that duration=15 parses to a 15-second duration.
- [ ] Run mvn -q -f clients/consumer/pom.xml test and observe the missing-class failure.
- [ ] Implement property parsing and a KafkaConsumer polling loop with auto.offset.reset=earliest.
- [ ] Run mvn -q -f clients/consumer/pom.xml test package.

### Task 4: Verify Build and Runbook

**Files:** root README, producer/README.md, consumer/README.md.

- [ ] Run clients/producer and clients/consumer test/package commands.
- [ ] Verify documented Maven commands accept duration, frequency, bootstrapServers, and groupId where applicable.
- [ ] Record output in progress.md and mark the persistent plan statuses complete.
