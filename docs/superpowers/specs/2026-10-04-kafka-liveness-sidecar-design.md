# Kafka Sidecar Liveness 扩展设计

## 目标

在不修改 Strimzi Operator 和 Kubernetes Kubelet 重启机制的前提下，将 Health Sidecar 的 Broker 健康判定接入 Kafka 容器的 liveness probe。Kafka 使用基于 Strimzi 镜像构建的自定义镜像，扩展 `/opt/kafka/kafka_liveness.sh`。

## 架构

Kafka Pod 增加一个 `emptyDir`，挂载到 `/var/run/kafka-health`。Health Sidecar 以读写方式挂载该目录，Kafka 容器以只读方式挂载同一目录。Sidecar 写入 `/var/run/kafka-health/status`，自定义 liveness 脚本读取该文件并与原 Strimzi liveness 结果合并。

Health Sidecar 复用当前集群的 `quay.io/strimzi/kafka:1.2.0-kafka-4.3.1` 镜像作为运行环境；状态写入命令仍须由 Sidecar 配置提供，镜像本身不包含 Broker 健康判定或本协议的状态写入逻辑。Kafka 容器使用基于该镜像构建的自定义镜像。

最终判断规则：

1. 先执行原 Strimzi liveness 检查并保存其退出码。
2. 只有共享状态文件中的状态为 `failure`，且时间戳存在、可解析并且不超过有效期时，才强制返回失败。
3. 状态文件不存在、不可读、格式非法、状态不是 `failure` 或状态已经过期时，返回原 Strimzi liveness 的退出码。
4. 因此，Sidecar 状态只能额外使 liveness 失败，不能把原有失败改成成功。

## 状态文件协议

状态文件采用单行键值格式，至少包含以下两行：

```text
status=failure
updated_at=1791072000
```

`updated_at` 使用 Unix epoch 秒。允许附加 `reason=...` 行，但 liveness 脚本不依赖该字段。默认有效期为 30 秒；有效期应由自定义镜像中的 `KAFKA_HEALTH_STATUS_TTL_SECONDS` 环境变量覆盖，未设置或非法时使用 30 秒。

Sidecar 必须先写入同一目录中的临时文件，再使用原子 `mv` 替换正式状态文件。Sidecar 恢复后必须持续刷新时间戳并写入当前状态，不能依赖旧的 `failure` 文件自动清除。

这是有意选择的 fail-open 行为：Sidecar 未启动、退出、停写、状态文件不可读或 `failure` 过期时，探针仅采用原 Strimzi 检查；原检查当前只确认 Java 进程存在，不代表 Broker 业务健康。持续故障必须持续刷新 `failure`，刷新间隔应短于有效期（建议不超过 10 秒）。当前 Pod 的 liveness 周期为 10 秒、失败阈值为 3 次；单次写入的 `failure` 可能在达到重启阈值前过期，不能以单次写入验证 Kubelet 重启。

部署时必须在创建 Kafka Pod 之前确保注入 Webhook 已就绪。已有 Pod 只能通过逐个重建获取新容器和挂载；每次等待新 Pod Ready 并核验镜像、容器、共享卷及挂载后，再处理下一个 Broker。若 Webhook 因 `failurePolicy: Ignore` 未能注入，不能仅以 Kafka Ready 判定部署成功。

## 权限与安全

- Kafka 的 volume mount 必须设置 `readOnly: true`。
- Health Sidecar 的 volume mount 必须可读写。
- Pod 的安全上下文必须保证 Sidecar 可以创建和原子替换状态文件，同时 Kafka 进程可以读取该文件。
- 状态文件目录只用于健康状态，不与 Kafka 数据目录或运行日志目录复用。

## 范围

本设计包含状态文件协议、Kafka 自定义镜像、liveness 脚本、Pod 共享目录和 Sidecar 注入链路。

本设计不实现 Broker 健康状态机本身；Health Sidecar 只需遵守上述状态文件协议即可接入。

## 验收标准

- 原 Strimzi liveness 通过且状态文件为新鲜 `failure` 时，Probe 失败。
- 原 Strimzi liveness 通过且状态文件为 `healthy`、缺失、非法或过期时，Probe 通过。
- 原 Strimzi liveness 失败时，无论 Sidecar 状态如何，Probe 都失败。
- Kafka 无法写入状态目录；Health Sidecar 可以原子更新状态文件。
- Kafka Pod 重建后共享状态目录为空，Sidecar 能重新建立状态。
- Sidecar 停写或状态过期时，仅回退到原探针；持续刷新 `failure` 后，Kubelet 连续失败达到阈值时会重启 Kafka 容器。
- 新建和逐个重建的每个 Kafka Pod 都具有预期的镜像、Health Sidecar、共享卷及挂载模式；缺少注入时验收失败。
