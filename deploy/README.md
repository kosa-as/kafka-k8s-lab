# 集群部署入口

`deploy` 目录是当前 Kafka 平台的统一部署入口。脚本按模块拆分，但共享仓库中的 YAML 和 Helm 配置。

## 脚本

- `all.ps1`：按依赖顺序部署完整平台。
- `kafka.ps1`：部署 Kafka Namespace、指标与运行日志 ConfigMap、Strimzi Operator，先安装并等待 Mutating Webhook，再构建自定义 Kafka 镜像并创建 KafkaNodePool、Kafka 和 Topic。
- `logging.ps1`：构建日志/健康 sidecar injector，创建运行日志 PVC，部署 TLS Webhook；`-InstallOnly` 只安装 Webhook，`-ReconcileOnly` 按 Broker 逐个检查并重建缺少完整注入形状的 Pod。
- `metrics.ps1`：配置 Kafka JMX Prometheus Exporter，并部署 Prometheus。
- `dashboard.ps1`：安装 Dashboard 7.14.0，使用 `dashboard/.helm-cache/7.14.0` 本地缓存 Helm Chart，暴露 HTTPS NodePort `30443`，并应用长期 Token Secret。

## 完整部署

在仓库根目录 `C:\Code\Kafka` 执行：

```powershell
.\deploy\all.ps1
```

支持 Windows PowerShell 5.1，无需安装 `pwsh`。OpenSSL 和 Helm 的可预期错误按进程退出码处理，正常的 stderr 进度输出不会中断部署。

执行顺序为：

```text
kafka.ps1 (含 Webhook 安装和 Kafka 创建) -> logging.ps1 -ReconcileOnly -> metrics.ps1 -> dashboard.ps1
```

脚本会等待 Strimzi Operator、Kafka、日志 injector、Prometheus 和 Dashboard 组件达到可用状态。首次执行可能需要拉取 Docker 和 Dashboard 镜像。

日志 injector 复用已有证书；更新 TLS 证书或本地镜像后会重新启动，并通过 API Server 的 server-side dry-run 确认 `log-agent`、`health-sidecar` 和健康目录挂载实际注入成功，再处理 Broker。检查不会创建真实 Pod。

自定义 Kafka 镜像在构建时将探针脚本规范为 LF，避免 Windows CRLF 导致 Linux 报 `env: 'sh\r': No such file or directory`。已有 Pod 仍使用原镜像时，需要重建 Pod 才能加载修正后的本地镜像；重建 Pod 不会删除数据和日志 PVC。

Dashboard Chart 会缓存到 `dashboard/.helm-cache/7.14.0`，后续运行优先复用本地 Chart，不再重复访问 GitHub。缓存缺失时下载失败，只有已有 Helm release 才会复用现有安装，并继续检查组件可用状态；首次安装下载失败仍会报错，错误信息会保留。该缓存目录已加入 `.gitignore`。

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
.\strimzi\verify-kafka-health-liveness.ps1
```

验证脚本会直接运行 liveness wrapper 的新鲜/过期状态测试，检查每个 Broker 的自定义镜像、`health-sidecar`、`kafka-health` `emptyDir` 及 Kafka 只读/侧车读写挂载，并通过 `kubectl exec` 确认 Kafka 只能读取健康目录。它不伪造 Broker 健康决策；持续写入 `status=failure` 后观察 Kubelet 3 次、10 秒周期的重启阈值属于单独的运维演练。

PowerShell 回归检查（在 Windows PowerShell 5.1 中执行；镜像检查需要已构建 `kafka-health:local`，下载回退和真实注入检查需要已部署的本地集群）：

```powershell
.\deploy\tests\logging-certificate.ps1
.\deploy\tests\kafka-image.ps1
.\deploy\tests\webhook-retry.ps1
.\deploy\tests\dashboard-download.ps1
.\deploy\tests\webhook-admission.ps1
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
