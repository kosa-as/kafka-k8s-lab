# Kafka Kubernetes Lab

基于 Strimzi 的 Kubernetes Kafka 实验与运维示例，当前面向 Docker Desktop Kubernetes。

## 包含内容

- `strimzi/`：3 节点 KRaft Kafka、KafkaTopic、运行日志 PVC、Log4j2 文件日志和 sidecar 注入器。
- `metric/`：Kafka JMX Prometheus Exporter 配置和 Prometheus 部署。
- `dashboard/`：Kubernetes Dashboard 访问说明和长期 Token Secret 清单。
- `deploy/`：统一部署入口，支持全量部署或按 Kafka、日志、指标、Dashboard 分模块更新。
- `clients/`：Java Producer/Consumer 示例。
- `docs/`：设计文档和实施记录。

## 快速部署

环境要求：Docker Desktop Kubernetes、`kubectl`、Helm、Docker 和 OpenSSL。

在仓库根目录执行：

```powershell
.\deploy\all.ps1
```

分模块更新：

```powershell
.\deploy\kafka.ps1
.\deploy\logging.ps1
.\deploy\metrics.ps1
.\deploy\dashboard.ps1
```

详细说明见 [deploy/README.md](deploy/README.md)。

## 主要访问地址

- Kafka 集群内地址：`kafka-kafka-bootstrap.kafka.svc:9092`
- Kubernetes Dashboard：<https://localhost:30443/>
- Prometheus：执行 `kubectl port-forward -n kafka svc/prometheus 9090:9090` 后访问 <http://localhost:9090/>

## 重要说明

- 当前 Docker Desktop `hostpath` StorageClass 不支持原地扩容；已绑定的 Kafka 数据 PVC 需要迁移才能从 20Gi 变为 40Gi。
- Dashboard 的 `dashboard-admin` 使用 `cluster-admin` 权限，仅适合本地实验环境。
- Kubernetes Dashboard 上游项目已归档；生产环境使用前应评估 Headlamp 等替代方案。
