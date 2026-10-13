# Bitnami Legacy Kafka 配置

本目录保存 Docker Desktop Kafka 集群的 Helm values 和 ConfigMap。`helm-values.yaml` 使用 `bitnamilegacy/kafka:4.0.0-debian-12-r10` 部署三个同时承担 KRaft controller 与 broker 角色的节点。Kafka Chart 32.5.0 的源码固定在 Git commit `bb5e98a6e3c5ca196420d2414769b681c9284101`；首次运行部署脚本时，同版本的 common 子 Chart 一起缓存在 `.helm-cache/`。Legacy 镜像是归档版本，不再获得更新。

## 配置文件

| 文件 | 用途 |
| --- | --- |
| [`helm-values.yaml`](helm-values.yaml) | 节点数量、镜像、资源、PVC、监听器、sidecar 和 JMX Exporter |
| [`00-namespace.yaml`](00-namespace.yaml) | 创建 `kafka` 命名空间 |
| [`00-jmx-config.yaml`](00-jmx-config.yaml) | JMX MBean 到 Prometheus 指标的映射规则 |
| [`10-log4j2-config.yaml`](10-log4j2-config.yaml) | 控制台与滚动文件日志配置 |

每个 Kafka Pod 包含 `kafka`、`jmx-exporter`、`log-agent`、`health-sidecar` 四个容器。Kafka 的存活和就绪由 Chart 的探针判断；`health-sidecar` 每五秒写一次状态文件，仅供实验观察。JMX Exporter 在 `9404/metrics` 暴露指标，具体查询方式见[指标说明](../metric/README.md)。

## 存储与 KRaft 身份

每个节点使用一个 20 GiB 的 `hostpath` 数据 PVC 和一个 2 GiB 的日志 PVC。Kafka 将运行日志同时写入 stdout 和 `/opt/bitnami/kafka/logs/server.log`；`log-agent` 只读挂载日志卷，并把文件内容输出到自身 stdout。日志文件按 100 MB 滚动，最多保留 10 个归档。

`deploy/kafka.ps1` 创建 `bitnami-kafka-kraft-static` Secret，保存 cluster ID 和三个节点的目录 ID。升级时脚本会读取已有节点的 `meta.properties`，以磁盘上的目录 ID 校正 Secret，然后运行 Helm。保留 PVC 时应同时保留这个 Secret；不要随意重新生成集群身份。当前部署不迁移其他集群的数据或消费者 offset。

`demo` topic 由部署脚本创建：3 个分区、副本因子 3、最小同步副本数 2、保留 7 天。Kafka 的默认副本因子和内部 topic 副本因子也设置为 3。

## 部署

从仓库根目录运行：

```powershell
.\deploy\kafka.ps1
.\deploy\metrics.ps1
```

完整平台入口为 `.\deploy\all.ps1`。修改 JMX 规则后运行 `.\deploy\metrics.ps1`；修改 Log4j2 配置后运行 `.\deploy\logging.ps1`。这些脚本在配置发生变化时重启相关工作负载并等待就绪。

## 连接地址

| 客户端位置 | Bootstrap 地址 |
| --- | --- |
| Kubernetes 集群内 | `bitnami-kafka.kafka.svc:9092` |
| Docker Desktop 宿主机 | `localhost:32000` |

宿主机还需能访问三个 broker 的 `localhost:32001`、`localhost:32002` 和 `localhost:32003`。监听器使用 PLAINTEXT，仅适合本地实验。

`localhost:32004` 是 chart 为集群内客户端 listener 创建的 NodePort；宿主机示例程序应使用对外 bootstrap `localhost:32000`，让元数据返回可从宿主机访问的 broker 地址。Kafka controller 通信使用集群内部端口。

## 检查

```powershell
kubectl -n kafka get statefulset,pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server bitnami-kafka:9092 --describe --topic demo
kubectl -n kafka logs bitnami-kafka-controller-0 -c log-agent --tail=20
kubectl -n kafka exec bitnami-kafka-controller-0 -c health-sidecar -- cat /var/run/kafka-health/status
```

三个 Pod 应各为 `4/4` 就绪，六个 PVC 应为 `Bound`；`demo` 的三个分区应各有三个同步副本。Prometheus 的抓取目标是三个 Kafka Pod 的 headless DNS 地址，端口为 `9404`。完整使用说明见仓库根目录的 [README](../README.md)。
