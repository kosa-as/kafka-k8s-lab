# Kafka 指标与 Prometheus

本目录部署一个独立的 Prometheus，用于抓取 Bitnami Kafka 的三个 JMX Exporter。每个 Kafka Pod 中的 `jmx-exporter` 容器通过本地 `5555` JMX 端口读取 Kafka MBean，并在 `9404/metrics` 提供 Prometheus 文本格式指标。Prometheus 自带查询和图形页面，无需额外安装 Grafana。

## 文件与数据流

| 文件 | 作用 |
| --- | --- |
| [`../bitnami/00-jmx-config.yaml`](../bitnami/00-jmx-config.yaml) | JMX Exporter 的 MBean 匹配与指标命名规则 |
| [`../bitnami/helm-values.yaml`](../bitnami/helm-values.yaml) | 启用 exporter sidecar 及 `9404` 端口 |
| [`10-prometheus-config.yaml`](10-prometheus-config.yaml) | 配置三个静态抓取目标，抓取间隔 15 秒 |
| [`20-prometheus.yaml`](20-prometheus.yaml) | Prometheus Deployment 和集群内 Service |

抓取路径为 `/metrics`，目标地址如下：

```text
bitnami-kafka-controller-0.bitnami-kafka-controller-headless.kafka.svc:9404
bitnami-kafka-controller-1.bitnami-kafka-controller-headless.kafka.svc:9404
bitnami-kafka-controller-2.bitnami-kafka-controller-headless.kafka.svc:9404
```

Prometheus 镜像固定为 `prom/prometheus:v3.5.0`，在 `kafka` 命名空间运行，Service 类型为 `ClusterIP`。TSDB 使用 `emptyDir`，数据保留时间为 6 小时；Pod 重建会清空本地指标历史。这适合本地实验，不能用作持久监控存储。

## 部署与更新

从仓库根目录运行：

```powershell
.\deploy\metrics.ps1
```

该脚本应用 JMX ConfigMap。如果规则发生变化，会滚动重启三个 Kafka Pod；随后应用 Prometheus 的 ConfigMap 和 Deployment，重启 Prometheus 并等待就绪。完整平台可运行 `.\deploy\all.ps1`。如果只更新抓取配置，也应使用上述脚本，使 Prometheus 重新加载 ConfigMap。

## 打开查询页面

在独立的 PowerShell 窗口保持端口转发运行：

```powershell
kubectl -n kafka port-forward svc/prometheus 9090:9090
```

然后打开 [Graph](http://localhost:9090/graph) 输入 PromQL，或打开 [Targets](http://localhost:9090/targets) 查看三个 `kafka-brokers` 目标。关闭端口转发窗口只会停止本机页面访问，不会停止集群中的抓取。

不使用浏览器也可以检查：

```powershell
kubectl -n kafka get deployment,svc -l app=prometheus
kubectl -n kafka exec deployment/prometheus -- wget -qO- 'http://localhost:9090/api/v1/targets?state=active'
```

三个目标的 `health` 应为 `up`，`lastError` 应为空。

## 常用查询

以下名称已在当前 Bitnami Kafka 4.0.0 实例中观察到。Topic、分区和运行状态改变后，某些指标及标签可能变化。

```promql
# 每个 Kafka 节点是否可被 Prometheus 抓取；预期三条序列的值均为 1
up{job="kafka-brokers"}

# JMX Exporter 是否抓取失败；预期值为 0
jmx_scrape_error{job="kafka-brokers"}

# 副本状态：欠复制分区数
kafka_server_replicamanager_underreplicatedpartitions

# 副本状态：低于最小同步副本数的分区数
kafka_server_replicamanager_underminisrpartitioncount

# demo topic 各分区的日志末端 offset
kafka_log_logendoffset{topic="demo"}

# Broker 收到的消息计数指标
kafka_server_brokertopicmetrics_messagesin
```

需要查看当前实际导出的指标名称时，在端口转发窗口运行期间执行：

```powershell
(Invoke-RestMethod 'http://localhost:9090/api/v1/label/__name__/values').data |
  Where-Object { $_ -match '^(kafka_|jmx_)' }
```

## 排查

1. `up` 为 `0` 时，先检查 `kubectl -n kafka get pods`，再打开 Targets 页查看该目标的 `lastError`。
2. Pod 正常但 exporter 报错时，检查 `kubectl -n kafka logs bitnami-kafka-controller-0 -c jmx-exporter --tail=100` 和 `jmx_scrape_error`。
3. 规则调整后指标仍未变化时，重新运行 `.\deploy\metrics.ps1`，确认 Kafka StatefulSet 和 Prometheus Deployment 已完成滚动更新。
4. 某条 Kafka 指标没有数据时，先从 API 读取当前指标名称；不要把旧版本或其他 exporter 的指标名直接套用到本配置。
