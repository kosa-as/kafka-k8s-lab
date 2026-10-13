# Kafka Kubernetes Lab

基于 Bitnami Legacy Kafka 4.0.0 的 Kubernetes Kafka 实验与运维示例，当前面向 Docker Desktop Kubernetes。

## 包含内容

- `bitnami/`：3 节点 KRaft Kafka Helm 配置、JMX Exporter、运行日志和 sidecar。
- `metric/`：Prometheus 部署和抓取配置。
- `dashboard/`：Kubernetes Dashboard 访问说明和长期 Token Secret 清单。
- `deploy/`：统一部署入口，支持全量部署或按 Kafka、日志、指标、Dashboard 分模块更新。
- `clients/`：Java Producer/Consumer 示例。
- `docs/`：设计文档和实施记录。

## 快速部署

环境要求：Docker Desktop Kubernetes、`kubectl` 和 Helm。首次部署需能访问 Docker Hub Legacy 镜像及 GitHub Chart 源码。

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

- Kafka 集群内地址：`bitnami-kafka.kafka.svc:9092`
- Windows 客户端：`localhost:32000`（broker 使用 `localhost:32001` 至 `32003`）
- Kubernetes Dashboard：<https://localhost:30443/>
- Prometheus：执行 `kubectl port-forward -n kafka svc/prometheus 9090:9090` 后访问 <http://localhost:9090/>

## 重要说明

- Bitnami Legacy 镜像是官方归档版本，不再获得更新；本仓库固定使用 Kafka `4.0.0-debian-12-r10`。
- 当前 Docker Desktop `hostpath` StorageClass 不支持原地扩容；新 Kafka 数据 PVC 各为 20Gi。
- Dashboard 的 `dashboard-admin` 使用 `cluster-admin` 权限，仅适合本地实验环境。
- Kubernetes Dashboard 上游项目已归档；生产环境使用前应评估 Headlamp 等替代方案。
