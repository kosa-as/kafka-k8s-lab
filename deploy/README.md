# 集群部署入口

`deploy` 目录是当前 Kafka 平台的统一部署入口。脚本按模块拆分，但共享仓库中的 YAML 和 Helm 配置。

## 脚本

- `all.ps1`：按依赖顺序部署完整平台。
- `kafka.ps1`：部署 Kafka Namespace、指标与运行日志 ConfigMap、运行日志 PVC、Strimzi Operator、KafkaNodePool、Kafka 和 Topic。
- `logging.ps1`：构建日志 sidecar injector，创建运行日志 PVC，部署 TLS Webhook，并应用 Kafka 文件日志配置；如果现有 Broker 未包含 `log-agent`，脚本会删除这些 Pod，让 Webhook 在重建时注入 sidecar。
- `metrics.ps1`：配置 Kafka JMX Prometheus Exporter，并部署 Prometheus。
- `dashboard.ps1`：安装 Dashboard 7.14.0，暴露 HTTPS NodePort `30443`，并应用长期 Token Secret。

## 完整部署

在仓库根目录 `C:\Code\Kafka` 执行：

```powershell
.\deploy\all.ps1
```

执行顺序为：

```text
kafka.ps1 -> logging.ps1 -> metrics.ps1 -> dashboard.ps1
```

脚本会等待 Strimzi Operator、Kafka、日志 injector、Prometheus 和 Dashboard 组件达到可用状态。首次执行可能需要拉取 Docker 和 Dashboard 镜像。

## 单模块更新

只更新某个模块时执行对应脚本：

```powershell
.\deploy\kafka.ps1
.\deploy\logging.ps1
.\deploy\metrics.ps1
.\deploy\dashboard.ps1
```

模块依赖如下：

- `kafka.ps1` 是基础入口；首次部署应先执行它。KafkaNodePool 挂载日志 PVC，Kafka CR 引用日志配置，因此脚本会先创建这些启动依赖。
- `logging.ps1` 依赖 Kafka Namespace、KafkaNodePool 和 Kafka CR 已存在。
- `metrics.ps1` 依赖 Kafka 已存在，并会等待 Kafka Ready 后部署 Prometheus。
- `dashboard.ps1` 与 Kafka/Prometheus 业务资源基本独立，只依赖可用的 Kubernetes API 和 Helm。

## 配置文件位置

- Kafka/Strimzi：`strimzi/`
- Prometheus：`metric/`
- Dashboard：`dashboard/`

部署脚本只负责编排；资源详情仍维护在各自目录的 YAML 文件中。

## 验证

```powershell
kubectl get kafka,kafkanodepool,kafkatopic,pods,pvc,svc -n kafka -o wide
kubectl get pods,svc -n kubernetes-dashboard -o wide
helm list -A
```

Dashboard 地址：<https://localhost:30443/>。Prometheus 默认通过端口转发访问：

```powershell
kubectl port-forward -n kafka svc/prometheus 9090:9090
```

## 兼容入口

以下旧路径仍保留，但只是兼容包装器，实际调用 `deploy` 下的新脚本：

- `strimzi/install.ps1` -> `deploy/kafka.ps1`
- `strimzi/install-runtime-log-sidecar.ps1` -> `deploy/logging.ps1`

新部署和日常更新请优先使用 `deploy` 目录入口。
