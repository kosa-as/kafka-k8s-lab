# 集群部署入口

所有命令均从仓库根目录运行，目标 Kubernetes context 为 `docker-desktop`。Kafka 使用 Bitnami Chart 32.5.0 和 `bitnamilegacy/kafka:4.0.0-debian-12-r10`。Chart 源码与 common 依赖锁定在同一 Git commit，并缓存在 `bitnami/.helm-cache/`；首次安装需要从 GitHub 下载源码、从 Docker Hub 拉取 Legacy 镜像。

## 脚本与执行顺序

| 脚本 | 作用 | 何时运行 |
| --- | --- | --- |
| [`kafka.ps1`](kafka.ps1) | 部署 3 节点 KRaft Kafka、PVC、JMX Exporter 和两个 sidecar；创建或更新 `demo` topic | 首次部署、修改 Helm values、升级 Kafka |
| [`metrics.ps1`](metrics.ps1) | 更新 JMX 规则及 Prometheus 抓取配置，等待相关 Pod 就绪 | 首次部署、修改指标规则或抓取配置 |
| [`logging.ps1`](logging.ps1) | 更新 Log4j2 配置，检查三个节点的日志文件和 `log-agent` | 修改日志规则后 |
| [`dashboard.ps1`](dashboard.ps1) | 安装或更新 Kubernetes Dashboard 与本地管理员身份 | 需要 Dashboard 时 |
| [`all.ps1`](all.ps1) | 依次运行 Kafka、指标、Dashboard 脚本 | 首次部署完整平台或统一更新 |

日志 sidecar 已由 Kafka Helm values 配置，完整入口不需要再单独执行 `logging.ps1`。Chaos Mesh 不包含在 `all.ps1` 中，也不会自动运行故障实验。

## 安装完整平台

先确认 Docker Desktop Kubernetes 已启动，`kubectl config current-context` 输出 `docker-desktop`，并已安装 `kubectl`、Helm 和 PowerShell。运行：

```powershell
kubectl config current-context
.\deploy\all.ps1
```

只需要 Kafka 与 Prometheus 时：

```powershell
.\deploy\kafka.ps1
.\deploy\metrics.ps1
```

脚本重复运行会使用同一 Helm release 和 KRaft Secret；已有数据 PVC 不会由这些脚本清空。`kafka.ps1` 会检查 cluster ID 与已有磁盘元数据是否一致；如果不一致，它会停止并报告错误，避免对错误的存储身份继续升级。

Kafka 使用六个 PVC：三个节点各有 20 GiB 数据卷和 2 GiB 运行日志卷。`demo` topic 为 3 分区、3 副本、最小同步副本数 2。宿主机 bootstrap 为 `localhost:32000`；broker 对外端口为 `32001` 至 `32003`。集群内地址为 `bitnami-kafka.kafka.svc:9092`。

## 验证

```powershell
.\deploy\tests\bitnami-render.ps1
kubectl -n kafka get statefulset,pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server bitnami-kafka:9092 --describe --topic demo
kubectl -n kafka logs bitnami-kafka-controller-0 -c log-agent --tail=20
kubectl -n kafka exec deployment/prometheus -- wget -qO- http://localhost:9090/api/v1/targets
```

预期 Kafka StatefulSet 为 `3/3`，Kafka Pod 各为 `4/4`，PVC 均为 `Bound`。Prometheus targets 中的三个 `kafka-brokers` 实例应为 `up`。查看图形页面时，运行 `kubectl -n kafka port-forward svc/prometheus 9090:9090`，然后打开 <http://localhost:9090/graph>。

## 变更与排查

- `helm-values.yaml` 的修改由 `kafka.ps1` 应用；修改 JMX 或 Log4j2 ConfigMap 后分别运行 `metrics.ps1` 或 `logging.ps1`。滚动更新期间可能短暂影响测试流量。
- `ImagePullBackOff` 时查看 `kubectl -n kafka describe pod bitnami-kafka-controller-0`；首次部署依赖镜像仓库和 Chart 源码下载。
- Kafka CLI 若出现本地 JMX 端口占用，使用本目录示例中的 `env -u JMX_PORT` 前缀。
- Dashboard Chart 下载失败时，`dashboard.ps1` 可在已有 release 的情况下继续复用它；详细行为见[Dashboard 说明](../dashboard/README.md)。

连接方式、Java 示例和常见问题见仓库根目录的 [README](../README.md)，各组件配置见 [Bitnami](../bitnami/README.md) 和 [指标](../metric/README.md) 说明。
