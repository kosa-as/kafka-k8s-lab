# Docker Desktop Kafka 实验环境

本仓库在 Docker Desktop 的 Kubernetes 集群中部署一个三节点 Kafka 实验环境。Kafka 使用 `bitnamilegacy/kafka:4.0.0-debian-12-r10`，由 Bitnami Kafka Helm Chart 32.5.0 管理。三个 Pod 均同时承担 KRaft controller 和 broker 角色；仓库还提供 Prometheus 指标采集、运行日志 sidecar、健康状态 sidecar、Java 生产者与消费者，以及可选的 Dashboard 和 Chaos Mesh 示例。

本配置面向单机开发和故障演练。Kafka 监听器使用 PLAINTEXT，Dashboard 的本地管理员拥有集群管理权限；请勿将这些端口直接暴露到不可信网络。Bitnami Legacy 镜像是归档镜像，不再获得更新。

## 环境要求

- Windows PowerShell、Docker Desktop，并启用 Docker Desktop Kubernetes。
- `kubectl` 当前 context 为 `docker-desktop`；部署脚本会检查这一点。
- Helm 命令行工具（当前环境使用 4.3.0）。首次运行需能从 GitHub 下载固定版本的 Chart 源码，并从 Docker Hub 拉取 Legacy 镜像。
- 运行 Java 示例需要 JDK 17 或更新版本及 Maven。
- Docker Desktop 应为三个 Kafka 节点及其 PVC 留出足够的 CPU、内存和磁盘空间。每个 Kafka 容器请求 1 CPU、2 GiB 内存，限制为 3 CPU、4 GiB 内存；每个节点的数据 PVC 为 20 GiB，运行日志 PVC 为 2 GiB，此外还需容纳 sidecar 和其他组件。

先确认集群：

```powershell
kubectl config current-context
kubectl get nodes
```

## 部署

在仓库根目录执行完整入口：

```powershell
.\deploy\all.ps1
```

该入口依次部署 Kafka、Prometheus 和 Kubernetes Dashboard。只需要 Kafka 与指标时运行：

```powershell
.\deploy\kafka.ps1
.\deploy\metrics.ps1
```

按需更新运行日志配置或 Dashboard：

```powershell
.\deploy\logging.ps1
.\deploy\dashboard.ps1
```

Kafka 脚本将 Chart 源码锁定到 Git commit `bb5e98a6e3c5ca196420d2414769b681c9284101`，缓存到被 Git 忽略的 `bitnami/.helm-cache/`。它还创建并维护 `bitnami-kafka-kraft-static` Secret，保证重复执行时保留 KRaft cluster ID 和节点目录 ID。Helm release 名为 `bitnami-kafka`，命名空间为 `kafka`。详细参数见 [Kafka Helm 配置](bitnami/helm-values.yaml)和[部署脚本说明](deploy/README.md)。

## 连接地址与测试 Topic

| 使用位置 | Bootstrap 地址 |
| --- | --- |
| Kubernetes 集群内 | `bitnami-kafka.kafka.svc:9092` |
| Docker Desktop 宿主机 | `localhost:32000` |

对宿主机客户端，broker 对外公布 `localhost:32001`、`localhost:32002`、`localhost:32003`。客户端从 bootstrap 地址获取元数据后还需要连接这些 broker 端口，因此四个 NodePort 都必须可用。集群内客户端使用 `bitnami-kafka.kafka.svc:9092`，无需端口转发。

部署脚本创建 `demo` topic：3 个分区、副本因子 3、最小同步副本数 2、保留时间 7 天。每个节点分别使用 20 GiB 数据 PVC 和 2 GiB 日志 PVC；Docker Desktop 的 `hostpath` StorageClass 不支持原地扩容。当前部署不执行数据迁移。

## 验证 Kafka

```powershell
kubectl -n kafka get statefulset,pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server bitnami-kafka:9092 --describe --topic demo
.\deploy\tests\bitnami-render.ps1
```

预期 StatefulSet 为 `3/3`，三个 Pod 各有四个容器就绪，六个 PVC 为 `Bound`；`demo` 的三个分区应显示三个副本均在 ISR 中。Kafka CLI 前使用 `env -u JMX_PORT`，避免临时 Java 进程与 broker 的 JMX 端口冲突。

## Java 生产者与消费者

两个独立的 Maven 示例位于 `clients/`。在仓库根目录运行：

```powershell
Set-Location .\clients
mvn test
mvn -pl producer compile exec:java '-Dduration=30' '-Dfrequency=2' '-DbootstrapServers=localhost:32000' '-Dtopic=demo'
mvn -pl consumer compile exec:java '-Dduration=30' '-DbootstrapServers=localhost:32000' '-Dtopic=demo' '-DgroupId=demo-consumer'
```

生产者发送以毫秒表示的时间戳；消费者从 `demo` 读取消息。建议在两个 PowerShell 窗口分别运行。示例程序的日志写入 `clients/logs/`。更多参数见[客户端说明](clients/README.md)。

## 指标、日志与健康状态

- 每个 Kafka Pod 都有一个 JMX Exporter 容器，在 `9404/metrics` 输出指标。Prometheus 抓取三个 Pod 的内部地址。
- Kafka 将运行日志写入日志 PVC 上的 `server.log`；`log-agent` sidecar 读取该文件并输出到自己的 stdout。
- `health-sidecar` 每五秒更新状态文件。Kafka 的实际存活与就绪仍由 Helm Chart 配置的探针判断；该状态文件不能替代 Kafka 请求或指标检查。

查看日志和健康状态：

```powershell
kubectl -n kafka logs bitnami-kafka-controller-0 -c kafka --tail=30
kubectl -n kafka logs bitnami-kafka-controller-0 -c log-agent --tail=30
kubectl -n kafka exec bitnami-kafka-controller-0 -c health-sidecar -- cat /var/run/kafka-health/status
```

查看 Prometheus 抓取目标，或在本机打开 Graph 页面：

```powershell
kubectl -n kafka exec deployment/prometheus -- wget -qO- 'http://localhost:9090/api/v1/targets?state=active'
kubectl -n kafka port-forward svc/prometheus 9090:9090
```

端口转发运行期间访问 <http://localhost:9090/graph>；可以查询 `up{job="kafka-brokers"}` 和 `jmx_scrape_error`。三个目标应为 `up`，错误值应为 `0`。指标配置见[指标说明](metric/README.md)。

Prometheus 当前使用临时 `emptyDir` 存储并保留 6 小时指标；Pod 重建后不会保留历史曲线。指标名称以当前 exporter 的实际输出为准，具体查询和排查命令见指标说明。

## 其他组件与目录

| 目录 | 内容 |
| --- | --- |
| [`bitnami/`](bitnami/README.md) | Kafka Helm values、JMX 和 Log4j2 配置 |
| [`deploy/`](deploy/README.md) | 完整部署与模块更新脚本 |
| [`metric/`](metric/README.md) | Prometheus 配置与查询方式 |
| [`clients/`](clients/README.md) | Java 生产者和消费者 |
| [`dashboard/`](dashboard/README.md) | Kubernetes Dashboard 的安装和登录说明 |
| [`chaos-mesh/`](chaos-mesh/README.md) | 可选故障注入示例；不会被完整部署入口自动执行 |

Dashboard 的本机入口为 <https://localhost:30443/>。它使用自签名证书，登录方式见对应说明。Chaos Mesh 示例可能中断 Kafka 服务，使用前请先阅读各实验的作用范围和恢复步骤。

通过 Maven 运行 Java 示例时，请按照[客户端说明](clients/README.md)显式传入所有参数；虽然 Java 配置类有默认值，当前 Maven 插件仍可能将省略的属性传成空值。

## 常见问题

| 现象 | 排查方法 |
| --- | --- |
| `ImagePullBackOff` | 检查 Docker Hub 可达性，以及 Pod 事件：`kubectl -n kafka describe pod bitnami-kafka-controller-0`。 |
| 宿主机客户端连接超时 | 检查 `kubectl -n kafka get svc` 中的 `32000` 至 `32003` NodePort，并确认 Docker Desktop Kubernetes 正常运行。 |
| Topic 有副本不在 ISR | 查看三个 Kafka Pod 状态和日志；副本追赶完成前，不要仅凭 Pod `Running` 判断集群恢复。 |
| Prometheus 指标缺失 | 先检查 `kubectl -n kafka get pods`，再查看 Prometheus targets 和 `jmx-exporter` 容器日志。 |
| 日志文件没有更新 | 查看 `log-agent` 输出，并执行 `.\deploy\logging.ps1` 检查各节点的日志文件。 |
