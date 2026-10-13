# Bitnami Legacy Kafka 配置

`helm-values.yaml` 使用 `bitnamilegacy/kafka:4.0.0-debian-12-r10` 部署三个 KRaft controller/broker 混合节点。部署脚本将 Kafka Chart 32.5.0 的源码固定在 Git commit `bb5e98a6e3c5ca196420d2414769b681c9284101`，连同同一版本的 common 子 Chart 缓存在 `.helm-cache/`。Legacy 镜像是归档版本，不再获得更新。

脚本创建 `bitnami-kafka-kraft-static` Secret，保存 cluster ID 和三个节点的目录 ID，供重复部署使用。每个节点使用 20 GiB 的 `hostpath` 数据 PVC 和 2 GiB 日志 PVC。`demo` topic 有 3 个分区、副本因子 3、最小同步副本数 2，保留 7 天。

Kafka 同时输出 stdout 和滚动的 `server.log`。`log-agent` sidecar 读取日志文件并输出到自身 stdout；`health-sidecar` 更新状态文件。Kafka 的存活与就绪由 Chart 提供的探针判断。JMX Exporter 在每个 Pod 的 `9404` 端口提供指标。

## 部署

从仓库根目录运行：

```powershell
.\deploy\kafka.ps1
.\deploy\metrics.ps1
```

完整平台入口为 `.\deploy\all.ps1`。指标、日志配置分别由 `.\deploy\metrics.ps1` 和 `.\deploy\logging.ps1` 更新。

## 连接地址

| 客户端位置 | Bootstrap 地址 |
| --- | --- |
| Kubernetes 集群内 | `bitnami-kafka.kafka.svc:9092` |
| Docker Desktop 宿主机 | `localhost:32000` |

宿主机还需能访问三个 broker 的 `localhost:32001`、`localhost:32002` 和 `localhost:32003`。监听器使用 PLAINTEXT，仅适合本地实验。

## 检查

```powershell
kubectl -n kafka get statefulset,pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server bitnami-kafka:9092 --describe --topic demo
kubectl -n kafka logs bitnami-kafka-controller-0 -c log-agent --tail=20
```

Prometheus 的抓取目标是三个 Kafka Pod 的 headless DNS 地址，端口为 `9404`。完整使用说明见仓库根目录的 [README](../README.md)。
