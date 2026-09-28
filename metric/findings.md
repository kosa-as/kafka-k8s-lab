# Findings & Decisions: Kafka Metrics

## Requirements

- Enable Kafka JMX metrics in the current Docker Desktop Kubernetes cluster.
- Provide a Prometheus scrape path and use the Prometheus Graph page to view metric graphs.
- Do not add Grafana.
- Prefer the simplest Strimzi-compatible setup; avoid a JMX Exporter sidecar unless verification shows it is required.
- Keep plan and future metrics-related files under `C:\Code\Kafka\metric`.

## Research Findings

- The running Kafka custom resource reports Kafka `4.3.1`; its `operatorLastSuccessfulVersion` is `1.2.0`.
- The Kafka broker currently has `KAFKA_JMX_EXPORTER_ENABLED=false`; no exporter HTTP port is declared in the broker container ports.
- The running cluster has three broker pods named `kafka-kafka-0`, `kafka-kafka-1`, and `kafka-kafka-2`.
- The in-cluster broker service is `kafka-kafka-brokers` in namespace `kafka`; verify the exact DNS/port to scrape from generated resources after enabling metrics.
- No Prometheus or ServiceMonitor/PodMonitor resources were found in the cluster at inspection time.
- The Prometheus UI can be reached locally with `kubectl port-forward` once Prometheus is deployed; the intended validation query is `up`.
- The Docker Desktop screenshot showed Kubernetes running with about 6.41 GB allocated RAM. Kafka pods request 4 GiB each, so resource availability should be checked before adding Prometheus; use conservative Prometheus requests and storage retention.
- Live cluster context is `docker-desktop` on Kubernetes `v1.36.1`; the single node is Ready with allocatable `22` CPU and `24504488Ki` memory.
- The installed Kafka CRD explicitly supports `spec.kafka.metricsConfig` with `type: jmxPrometheusExporter` and `valueFrom.configMapKeyRef`; the alternate `strimziMetricsReporter` is also schema-valid but is not needed for Prometheus HTTP scraping.
- Current broker pods use image `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1`, have `KAFKA_JMX_EXPORTER_ENABLED=false`, and expose only Kafka ports (9090/9091/9092/9094) before metrics are enabled.
- A read-only `kubectl version --short` check is not valid with kubectl from this environment (`unknown flag: --short`); node and CRD inspection succeeded.
- Implementation outcome: all three brokers now expose JMX exporter metrics on port 9404; Prometheus v3.5.0 scrapes all three targets successfully, and the local Graph page is available through the active port-forward at `http://localhost:9090/graph`.
- The Kafka LogEndOffset rule was refined so topic and partition are Prometheus labels: `kafka_log_logendoffset{topic="demo",partition="0"}`. The old expanded names stop receiving current samples after the scrape transition, though Prometheus retains their historical series until retention expiry.

## Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Configure JMX Prometheus Exporter through Strimzi's Kafka `metricsConfig`. | Strimzi manages the exporter alongside the broker and avoids a separately maintained sidecar. Verify exact schema against the installed version before applying. |
| Run Prometheus independently in the cluster. | It can scrape all broker endpoints and serve its own Graph UI. |
| Use Prometheus Graph instead of Grafana. | It directly fulfills the current goal with fewer components. |
| Use explicit per-broker scrape targets unless a simpler, verified discovery mechanism is available. | A small single-cluster setup can be understood and debugged easily with explicit broker endpoints. |

## Issues / Verification Notes

- The prior discussion used “built-in Metrics Reporter” imprecisely. The intended Strimzi `metricsConfig` path is the JMX Prometheus Exporter managed for Kafka, not a separate Kafka Metrics Reporter.
- Exact `metricsConfig` schema, exporter port, generated service port, and metric names must be confirmed for Strimzi 1.2.0 and Kafka 4.3.1 before implementation.
- Maven is unrelated to this metrics setup; Kubernetes deployment and Prometheus scrape checks are the relevant verification.

## Resources

- Project Kafka CR: `strimzi/20-kafka.yaml`
- Strimzi installation/version values: `strimzi/install.ps1`, `strimzi/helm-values.yaml`
- Current cluster namespace: `kafka`
- Kafka metrics endpoint must be discovered from generated Strimzi resources after enabling `metricsConfig`.
