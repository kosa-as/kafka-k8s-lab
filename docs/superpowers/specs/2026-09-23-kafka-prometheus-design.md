# Kafka Prometheus Metrics Design

## Goal

Expose metrics from the live three-broker Strimzi Kafka cluster and make them queryable in the Prometheus Graph page at `http://localhost:9090/graph` without Grafana.

## Scope

- Enable Strimzi's built-in JMX Prometheus Exporter through `spec.kafka.metricsConfig`.
- Keep exporter rules in a ConfigMap in `metric/`.
- Deploy one small Prometheus workload in namespace `kafka`.
- Scrape broker endpoints over stable in-cluster DNS names.
- Expose Prometheus through a ClusterIP Service and document the port-forward command.

## Architecture and Data Flow

The Kafka CR references the exporter rules ConfigMap with `type: jmxPrometheusExporter`. Strimzi rolls the broker pods and exposes the exporter on port `9404` at `/metrics`. Prometheus uses three explicit targets, `kafka-kafka-0.kafka-kafka-brokers.kafka.svc:9404` through `kafka-kafka-2...`, and serves its UI on port `9090`. The local browser reaches that UI through `kubectl -n kafka port-forward svc/prometheus 9090:9090`.

## Configuration

- Use a pinned Prometheus image (`prom/prometheus:v3.5.0`).
- Use a 1Gi `emptyDir` for local data and `--storage.tsdb.retention.time=6h` for Docker Desktop.
- Request 100m CPU / 128Mi memory and limit 500m CPU / 512Mi memory.
- Scrape every 15 seconds with a 10 second timeout.
- Include standard Kafka broker MBeans and JVM/process metrics while excluding noisy per-topic/per-partition MBeans by default.

## Failure Handling

- `kubectl apply --dry-run=server` validates manifests before mutation.
- Kafka readiness must return before Prometheus target checks.
- The verification must prove all three broker targets are `up`, the `/metrics` endpoint returns Prometheus text, and a representative Kafka query returns a time series.
- If a broker rollout or image pull fails, stop and record the exact Kubernetes condition in `metric/progress.md`; do not delete existing Kafka resources.

## Out of Scope

Grafana, Prometheus Operator CRDs, persistent Prometheus storage, alerting rules, and external exposure of Prometheus.
