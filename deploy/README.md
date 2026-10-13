# 集群部署入口

当前入口使用 Bitnami Kafka Chart 32.5.0 与 `bitnamilegacy/kafka:4.0.0-debian-12-r10`。Chart 源码与 common 依赖锁定在同一 Git commit，并缓存在 `bitnami/.helm-cache/`。首次安装需要从 GitHub 下载源码及从 Docker Hub 拉取 Legacy 镜像。

## 脚本

- `kafka.ps1`：部署 3 节点 KRaft Kafka、JMX exporter、日志 PVC、`log-agent` 与 `health-sidecar`，并创建 `demo` topic。
- `logging.ps1`：更新 Log4j2 配置，并验证每个节点的日志文件与 `log-agent`。
- `metrics.ps1`：更新 JMX 配置和 Prometheus 抓取目标。
- `dashboard.ps1`：维护现有 Kubernetes Dashboard。
- `retire-strimzi.ps1`：新集群验证通过后，移除旧 Strimzi Operator、资源及其测试 PVC。
- `all.ps1`：按 Kafka、指标、Dashboard 的顺序部署平台；日志 sidecar 已随 Kafka 一起部署。

```powershell
.\deploy\all.ps1
```

Kafka 节点使用新 PVC，与旧 Strimzi 集群并行安装。部署成功后先检查客户端和 Prometheus，再运行旧集群清理脚本。清理会删除旧测试数据，无法恢复。

## 验证

```powershell
.\deploy\tests\bitnami-render.ps1
kubectl -n kafka get statefulset,pods,pvc,svc -l app.kubernetes.io/instance=bitnami-kafka
kubectl -n kafka exec bitnami-kafka-controller-0 -c kafka -- env -u JMX_PORT /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server bitnami-kafka:9092 --describe --topic demo
kubectl -n kafka logs bitnami-kafka-controller-0 -c log-agent --tail=20
kubectl -n kafka exec deployment/prometheus -- wget -qO- http://localhost:9090/api/v1/targets
```

宿主机 Kafka bootstrap 为 `localhost:32000`；broker 分别为 `localhost:32001` 至 `32003`。Prometheus 使用 `kubectl -n kafka port-forward svc/prometheus 9090:9090` 查看。

旧的 `strimzi/` 目录和相关测试脚本保留为历史实现参考，不再作为当前部署入口。
