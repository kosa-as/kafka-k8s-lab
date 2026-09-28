# Task Plan: Kafka Metrics and Prometheus Graph

## Goal

Enable Kafka broker JMX metrics in the current Docker Desktop Kubernetes cluster, scrape them with a lightweight Prometheus instance, and view/query the metrics through Prometheus Graph without Grafana.

## Next Step

Keep the verified Prometheus port-forward running for local browser access; no further Kubernetes changes are required.

## Current Phase

Phase 4: View and Verify Metrics

## Phases

### Phase 1: Version and Configuration Verification

- [x] Confirm the installed Strimzi version and Kafka 4.3.1 metricsConfig support.
- [x] Confirm the exporter configuration format, metrics port, and in-cluster broker DNS targets.
- [x] Check current Docker Desktop resource constraints for a lightweight Prometheus workload.
- **Status:** complete

### Phase 2: Enable Kafka JMX Metrics

- [x] Add a JMX Exporter rules ConfigMap under this directory.
- [x] Configure `spec.kafka.metricsConfig` in the Kafka custom resource using the verified Strimzi schema.
- [x] Apply the changes and verify all three broker pods expose Prometheus-format metrics.
- **Status:** complete

### Phase 3: Deploy Lightweight Prometheus

- [x] Add a standalone Prometheus ConfigMap and Deployment/Service manifests under this directory.
- [x] Configure scraping for each of the three Kafka broker metrics endpoints.
- [x] Use modest resource requests and a short local-retention policy suitable for Docker Desktop.
- [x] Apply the manifests and verify Prometheus discovers all brokers.
- **Status:** complete

### Phase 4: View and Verify Metrics

- [x] Port-forward the Prometheus Service to localhost.
- [x] Open the Prometheus Graph page and execute `up` to confirm scrape health.
- [x] Query representative Kafka broker metrics and confirm graph output.
- [x] Record exact commands, endpoint details, and observed results in progress.md.
- **Status:** complete

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Use Strimzi-managed `spec.kafka.metricsConfig` with the JMX Prometheus Exporter, not a separate sidecar. | Fewer moving parts and fits the existing Strimzi-managed Kafka deployment. |
| Deploy Prometheus as a separate lightweight Kubernetes workload. | Prometheus owns scrape scheduling and the Graph UI; it should not be coupled to each Kafka broker pod. |
| Do not deploy Grafana. | Prometheus's built-in Graph page is sufficient for interactive metric queries and time-series graphs. |
| Keep all metrics-related manifests and instructions under `metric/`. | Matches the requested project layout and keeps this setup isolated from the application modules. |

## Errors Encountered

| Error | Attempt | Resolution |
|-------|---------|------------|
