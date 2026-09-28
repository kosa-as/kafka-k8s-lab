# Strimzi Kafka 集群

本目录部署一个 3 节点的 Strimzi KRaft Kafka 集群，运行在当前 Docker Desktop Kubernetes 中。

## 资源

- 每个 Kafka 节点：4 CPU、4 GiB memory request、6 GiB memory limit、40 GiB PVC
- 每个 Kafka 节点另有独立的 2 GiB 运行日志 PVC，实际 Log4j2 文件日志限制为 100 MiB 当前文件和 10 个压缩归档
- 每个 Kafka 节点 JVM 堆：初始 2 GiB、最大 3 GiB（以当前 `20-kafka.yaml` 为准）
- Kafka 节点同时承担 `broker` 和 `controller` 角色
- 3 个节点使用 `hostpath` StorageClass 和独立 PVC
- 集群调度请求约为 12 CPU、12 GiB 内存，运行时上限为 18 GiB、60 GiB 磁盘，另需为 Kubernetes 和 Strimzi 预留资源

## 部署

在 PowerShell 中执行：

```powershell
.\deploy\kafka.ps1
```

启用 Kafka 运行日志文件和 sidecar：

```powershell
.\deploy\logging.ps1
```

完整部署请执行：

```powershell
.\deploy\all.ps1
```

旧的 `strimzi/install.ps1` 和 `strimzi/install-runtime-log-sidecar.ps1` 仍可使用，但现在只是调用 `deploy` 目录中的兼容入口。

`deploy/kafka.ps1` 会先创建 Kafka CR 引用的指标/运行日志 ConfigMap 和日志 PVC；`deploy/logging.ps1` 再构建并启用 sidecar 注入器及 Webhook。

该脚本在 Docker Desktop 中构建 `kafka-log-sidecar-injector:local`，创建三个日志 PVC，配置 Kafka 同时写 stdout 和 `/mnt/kafka-runtime-logs/server.log`，并通过 Mutating Admission Webhook 给每个 broker Pod 注入 `log-agent` sidecar。

注意：Docker Desktop 的 `hostpath` StorageClass 当前不支持扩容。Strimzi 会保留 KafkaNodePool 的 40 GiB 目标配置，但已经绑定的 `data-kafka-kafka-0/1/2` PVC 实际容量仍为 20 GiB。升级到支持扩容的 StorageClass，或在停机并完成数据迁移后，才能把现有数据卷实际扩大到 40 GiB。

Strimzi Operator 使用固定的 Helm Chart `1.2.0` 安装；本目录保存 Helm values 和 Kafka 集群自身的全部配置。

## 访问

- 集群内：`kafka-kafka-bootstrap.kafka.svc:9092`
- 宿主机：通过 `kafka-kafka-external-bootstrap` Service 的 NodePort 访问

查看宿主机端口：

```powershell
kubectl get svc kafka-kafka-external-bootstrap -n kafka
```

## 验证

```powershell
kubectl get kafka -n kafka
kubectl get pods,pvc -n kafka -o wide
kubectl get kafka kafka -n kafka -o jsonpath='{.status.conditions[*].message}'
```

查看 broker 文件日志和 sidecar 输出：

```powershell
kubectl -n kafka exec kafka-kafka-0 -c kafka -- ls -lh /mnt/kafka-runtime-logs
kubectl -n kafka exec kafka-kafka-0 -c kafka -- tail -n 20 /mnt/kafka-runtime-logs/server.log
kubectl -n kafka logs -f kafka-kafka-0 -c log-agent
```
