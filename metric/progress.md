# Progress Log

## Session: 2026-09-23

### Phase 1: Plan Initialization

- **Status:** complete
- Actions taken:
  - Read the `planning-with-files` skill instructions and templates.
  - Captured the previously inspected cluster facts and agreed design: Strimzi-managed JMX Prometheus Exporter, standalone Prometheus, Prometheus Graph, no Grafana.
  - Created the metrics planning directory and its three planning files.
- Files created/modified:
  - `metric/task_plan.md`
  - `metric/findings.md`
  - `metric/progress.md`

### Phase 2: Metrics Implementation

- **Status:** complete
- Actions taken:
  - Confirmed live context `docker-desktop`, Kubernetes `v1.36.1`, and a Ready 3-broker Kafka 4.3.1 cluster managed by Strimzi 1.2.0.
  - Confirmed the installed CRD schema accepts `spec.kafka.metricsConfig.type: jmxPrometheusExporter` with a ConfigMap key reference.
  - Confirmed broker pods currently have JMX exporter disabled and no metrics container port.
  - Confirmed node allocatable capacity is 22 CPU and 24504488Ki memory.
  - Added `00-kafka-metrics-config.yaml`, wired `strimzi/20-kafka.yaml`, and verified all three brokers expose port 9404 with JMX exporter HTTP output.
  - Added `10-prometheus-config.yaml` and `20-prometheus.yaml`; Prometheus deployment rolled out Ready with a ClusterIP service on 9090.
  - Started `kubectl port-forward -n kafka svc/prometheus 9090:9090` (PowerShell session remains active).
  - Prometheus `/api/v1/targets` reports all 3 `kafka-brokers` targets `health: up`; query `up` returns three series, each value `1`.
  - Query `jmx_exporter_build_info` returns exporter version 1.6.0 for all brokers; Kafka series are present (for example `kafka_log_log_logendoffset_topic_cluster_metadata_partition_0`).
  - Opened the local Prometheus Graph page and executed `up`; the Graph tab visibly shows all three broker series.
- Files created/modified:
  - `metric/00-kafka-metrics-config.yaml`
  - `metric/10-prometheus-config.yaml`
  - `metric/20-prometheus.yaml`
  - `metric/README.md`
  - `strimzi/20-kafka.yaml`
  - `metric/findings.md`
  - `metric/task_plan.md`

## Test Results

| Test | Input | Expected | Actual | Status |
|------|-------|----------|--------|--------|
| Confirm new planning files | List files under `metric` | Three planning files exist with the agreed scope | `task_plan.md`, `findings.md`, and `progress.md` are present | passed |
| Kubernetes server dry-run | `kubectl apply --dry-run=server` for all four manifests | API server accepts resources | ConfigMap, Kafka CR, Prometheus ConfigMap, Deployment, Service accepted | passed |
| Kafka readiness | `kubectl wait --for=condition=Ready kafka/kafka -n kafka` | Kafka Ready after exporter rollout | Condition met; all 3 broker pods Running/Ready | passed |
| Broker exporter | HTTP GET each broker `:9404/metrics` from a temporary curl pod | Prometheus text returned | All brokers returned JMX exporter metrics | passed |
| Prometheus targets | `GET http://localhost:9090/api/v1/targets` | Three healthy targets | 3 targets, all `health: up`, no errors | passed |
| Prometheus Graph | Graph query `up` in browser | Three broker series visible | Graph page shows broker 0/1/2 series | passed |

## Error Log

| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|
| 2026-09-23 | Kafka image has no `wget` binary | 1 | Used temporary `curlimages/curl` pods for endpoint checks |
| 2026-09-23 | Initial Kafka metric example query returned no series | 1 | Enumerated actual exported names and documented a live Kafka series plus regex discovery query |
| 2026-09-23 | Combined `kubectl get deploy prometheus,svc prometheus` used invalid resource syntax | 1 | Re-ran with explicit `deployment/prometheus service/prometheus` resource names |
| 2026-09-23 | Generic `kafka.log` rule encoded topic/partition into metric names | 1 | Added a specific `kafka_log_logendoffset` rule with `topic` and `partition` labels; after one scrape cycle the old name had zero current samples |

## 5-Question Reboot Check

| Question | Answer |
|----------|--------|
| Where am I? | Plan initialized; metrics implementation has not started. |
| Where am I going? | Verify Strimzi metrics schema, enable exporter, deploy Prometheus, verify Graph queries. |
| What's the goal? | View Kafka metrics graphs in Prometheus without Grafana. |
| What have I learned? | See `findings.md`. |
| What have I done? | Created `metric/task_plan.md`, `metric/findings.md`, and `metric/progress.md`. |
