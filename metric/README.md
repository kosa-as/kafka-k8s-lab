# Kafka Prometheus 指标

Bitnami Kafka Chart 在每个 Kafka Pod 内运行 `jmx-exporter` sidecar，使用 `bitnami/00-jmx-config.yaml` 中的规则从本地 JMX 端口采集，并在 `9404/metrics` 输出。Prometheus 部署由 `metric/20-prometheus.yaml` 提供，抓取目标在 `metric/10-prometheus-config.yaml` 中。

```text
bitnami-kafka-controller-0.bitnami-kafka-controller-headless.kafka.svc:9404
bitnami-kafka-controller-1.bitnami-kafka-controller-headless.kafka.svc:9404
bitnami-kafka-controller-2.bitnami-kafka-controller-headless.kafka.svc:9404
```

应用指标配置并检查：

```powershell
.\deploy\metrics.ps1
kubectl -n kafka exec deployment/prometheus -- wget -qO- http://localhost:9090/api/v1/targets
kubectl -n kafka port-forward svc/prometheus 9090:9090
```

Prometheus Graph 位于 <http://localhost:9090/graph>。可用 `up{job="kafka-brokers"}` 检查三个抓取目标，并用 `jmx_scrape_error` 检查 exporter 是否成功采集。具体指标名称以当前 exporter 的实际输出为准。
