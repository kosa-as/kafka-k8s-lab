FROM quay.io/strimzi/kafka:1.2.0-kafka-4.3.1

USER 0

RUN test -x /opt/kafka/kafka_liveness.sh \
    && cp /opt/kafka/kafka_liveness.sh /opt/kafka/kafka_liveness_strimzi.sh

COPY kafka_liveness.sh /opt/kafka/kafka_liveness.sh

# Windows checkouts can supply CRLF even when the repository stores LF.
# Kubelet executes the shebang directly, so normalize before installing it.
RUN sed -i 's/\r$//' /opt/kafka/kafka_liveness.sh \
    && chmod 0755 /opt/kafka/kafka_liveness.sh /opt/kafka/kafka_liveness_strimzi.sh

USER 1001
