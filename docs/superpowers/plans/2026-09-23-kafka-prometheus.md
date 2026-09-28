# Kafka Prometheus Metrics Implementation Plan

> **For agentic workers:** Execute the tasks in this plan sequentially in the current session. Configuration files are validated with Kubernetes server-side dry-run and live endpoint checks.

**Goal:** Enable broker metrics in the live Strimzi cluster and expose a local Prometheus Graph UI.

**Architecture:** Strimzi manages the JMX Prometheus Exporter on each broker. A standalone Prometheus deployment in namespace `kafka` statically scrapes the three broker DNS targets and is exposed by a ClusterIP Service for local port-forwarding.

**Tech Stack:** Kubernetes YAML, Strimzi Kafka CRD v1, JMX Prometheus Exporter, Prometheus v3.5.0, PowerShell/kubectl.

**Spec:** `docs/superpowers/specs/2026-09-23-kafka-prometheus-design.md`

## Global Constraints

- Keep metrics manifests and instructions under `metric/`.
- Do not deploy Grafana or Prometheus Operator.
- Use namespace `kafka` and exporter port `9404`.
- Keep Prometheus local-only through `kubectl port-forward`.

---

### Task 1: Add exporter configuration and wire Kafka CR

**Files:**
- Create: `metric/00-kafka-metrics-config.yaml`
- Modify: `strimzi/20-kafka.yaml`

- [ ] Add a ConfigMap named `kafka-metrics` with a `kafka-metrics-config.yml` key containing allow-listed JMX exporter rules for Kafka, JVM, and process metrics.
- [ ] Add `spec.kafka.metricsConfig.type: jmxPrometheusExporter` and `valueFrom.configMapKeyRef` to the Kafka CR.
- [ ] Run `kubectl apply --dry-run=server` for the ConfigMap and Kafka CR.

### Task 2: Add Prometheus workload

**Files:**
- Create: `metric/10-prometheus-config.yaml`
- Create: `metric/20-prometheus.yaml`

- [ ] Add a Prometheus ConfigMap with global scrape settings and three explicit broker targets on port 9404.
- [ ] Add a Deployment using `prom/prometheus:v3.5.0`, a 1Gi `emptyDir`, six-hour retention, conservative resources, and the ConfigMap mount.
- [ ] Add a ClusterIP Service exposing port 9090.
- [ ] Run `kubectl apply --dry-run=server` for both manifests.

### Task 3: Apply and verify the live cluster

**Files:**
- Create: `metric/README.md`
- Modify: `metric/task_plan.md`
- Modify: `metric/progress.md`

- [ ] Apply the exporter ConfigMap and Kafka CR; wait for `kafka/kafka` Ready and all broker pods Ready.
- [ ] Confirm each broker exposes `9404/metrics` using temporary `kubectl port-forward` checks.
- [ ] Apply Prometheus manifests and wait for the Deployment and Service endpoints.
- [ ] Port-forward `svc/prometheus` to `localhost:9090`, query `/api/v1/targets`, `up`, and a Kafka metric, and record observed output and the browser URL.
- [ ] Update the plan and progress ledgers with exact commands, statuses, and any errors.
