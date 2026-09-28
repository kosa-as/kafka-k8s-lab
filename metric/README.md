# Kafka Prometheus 指标

本目录为当前 Docker Desktop Kubernetes 中的 3 个 Strimzi Kafka Broker 启用 JMX Prometheus Exporter，并部署一个独立的 Prometheus。Prometheus 自带查询和 Graph 页面，因此不需要 Grafana。

## 部署内容

- `00-kafka-metrics-config.yaml`：JMX Exporter 规则 ConfigMap。
- `strimzi/20-kafka.yaml`：通过 `spec.kafka.metricsConfig` 引用上述 ConfigMap。
- `10-prometheus-config.yaml`：Prometheus 抓取配置，抓取 3 个 Broker 的 `9404/metrics`。
- `20-prometheus.yaml`：Prometheus Deployment 和 ClusterIP Service。
- [METRICS.md](METRICS.md)：从当前 Prometheus API 导出的完整指标名称清单。

Exporter 在每个 Broker 的 `9404` 端口提供 Prometheus 文本格式指标；Prometheus 使用以下 3 个集群内地址抓取：

```text
kafka-kafka-0.kafka-kafka-brokers.kafka.svc:9404
kafka-kafka-1.kafka-kafka-brokers.kafka.svc:9404
kafka-kafka-2.kafka-kafka-brokers.kafka.svc:9404
```

## 部署或重新部署

推荐使用统一部署入口：

```powershell
.\deploy\metrics.ps1
```

完整部署请执行：

```powershell
.\deploy\all.ps1
```

该脚本会先应用 Kafka 指标 ConfigMap，再等待 Kafka Ready，最后部署 Prometheus。

如需手动分步执行，命令如下：

在 `C:\Code\Kafka` 的 PowerShell 中执行：

```powershell
kubectl apply -f .\metric\00-kafka-metrics-config.yaml
kubectl apply -f .\strimzi\20-kafka.yaml
kubectl wait --for=condition=Ready kafka/kafka -n kafka --timeout=900s
kubectl apply -f .\metric\10-prometheus-config.yaml
kubectl apply -f .\metric\20-prometheus.yaml
kubectl rollout status deployment/prometheus -n kafka --timeout=180s
```

Kafka CR 更新后，Strimzi 可能会滚动重启 Broker；等待 `kafka/kafka` 回到 `Ready=True` 后再查询指标。

## 在本地浏览器打开

保持下面的端口转发命令运行：

```shell
kubectl port-forward -n kafka svc/prometheus 9090:9090
```

然后打开：

- [Prometheus Graph](http://localhost:9090/graph)：输入 PromQL 并查看图形。
- [Prometheus Targets](http://localhost:9090/targets)：确认 3 个 Broker 的抓取状态。

## 常用查询

```promql
# 3 个 Broker 是否都能被 Prometheus 抓到
up

# 查看所有 Kafka 指标
{__name__=~"kafka_.*"}

# Kafka Broker 收到的消息数（按当前实际存在的 topic/partition 展开）
kafka_server_brokertopicmetrics_messagesin

# Kafka 日志末端 offset（topic 和 partition 是标签）
kafka_log_logendoffset{topic="demo", partition="0"}

# JVM 堆和非堆内存
jvm_memory_used_bytes

# JMX Exporter 自身是否发生抓取错误
jmx_scrape_error
```

Kafka 指标会随着当前 Broker 的 MBean、topic 和 partition 变化而变化；完整的当前快照见 [METRICS.md](METRICS.md)。如需刷新清单，可重新运行端口转发后请求：

```powershell
(Invoke-RestMethod http://localhost:9090/api/v1/label/__name__/values).data
```
