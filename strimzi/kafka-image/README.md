# Custom Kafka liveness image

This image is built from `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` and keeps the
upstream Java-process probe at `/opt/kafka/kafka_liveness_strimzi.sh`. The
wrapper at `/opt/kafka/kafka_liveness.sh` runs that probe first and only adds a
failure when `/var/run/kafka-health/status` contains a fresh
`status=failure`/`updated_at=<unix-seconds>` pair.

Build it in Docker Desktop's image store:

```powershell
docker build -t kafka-health:local strimzi/kafka-image
docker run --rm --entrypoint /usr/bin/sh kafka-health:local -c "test -x /opt/kafka/kafka_liveness.sh && test -x /opt/kafka/kafka_liveness_strimzi.sh"
```

`KAFKA_HEALTH_STATUS_TTL_SECONDS` accepts a positive integer and defaults to
30 seconds for invalid or missing values. The injector writes the status file
atomically and refreshes it every five seconds. The Pod annotation
`kafka.strimzi.io/health-test-status: failure` controls the injected sidecar's
test state (`healthy` is the default); it is an integration-test control, not
a broker health decision. Set it on one broker's Pod template before a
restart-threshold exercise, then remove it and reconcile the Pod to restore
healthy status.
