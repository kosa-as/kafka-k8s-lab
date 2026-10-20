# Kafka 启动阶段 Ready 候选空窗：证据归档

本归档位于 `formula-method/design/KafkaBroker` 内，保存 2026-10-20 对本地 `docker-desktop` 集群的只读观察，以及 PAT 3.5.1 对 M0 组合模型给出的反例。目标是 `kafka/bitnami-kafka-controller-0` 中的 `kafka` 容器，镜像摘要和 Pod UID 见 [Pod 与探针状态快照](pod-probe-status.json)。所有时间均为 UTC。

## 归档文件

| 文件 | 内容与来源 |
| --- | --- |
| [kafka-startup-selected.log](kafka-startup-selected.log) | 从 `kubectl --context docker-desktop -n kafka logs bitnami-kafka-controller-0 -c kafka --timestamps` 输出的 2237 行中，按 Controller/Client 端口、Broker 追赶与 unfence、启动完成等关键词筛出的 **9 条原始行**。未改写日志文本；其余日志未纳入归档。 |
| [pod-probe-status.json](pod-probe-status.json) | 通过 `kubectl --context docker-desktop -n kafka get pod bitnami-kafka-controller-0 -o json` 只读提取的 Kafka 容器、三类探针和 Pod Ready 条件；记录了采集时间。 |
| [pat-gap-counterexample.txt](pat-gap-counterexample.txt) | 当前 [ReadinessGapLTL.csp](../ReadinessGapLTL.csp) 的 PAT 完整输出副本，包含 1 条预期 NOT valid 的反例和被包含的 Kubelet 断言结果。 |
| [model-inputs-sha256.json](model-inputs-sha256.json) | 生成反例时五份 CSP# 输入文件的 SHA-256，用于核对模型版本。 |
| [archive-files-sha256.json](archive-files-sha256.json) | 本归档其余文件的 SHA-256，供后续核验。 |

## 实际观察到的启动顺序

| 时间 | 观察 |
| --- | --- |
| 01:39:07.279442451 | Controller 9093 接收线程记录 `Awaiting socket connections`。 |
| 01:39:07.667490139 | Broker 开始等待 Controller 确认元数据追赶。 |
| 01:39:18.699303682 | Broker 完成上述等待。 |
| 01:39:18.740359089 | Broker 记录已解除 unfence，状态从 RECOVERY 进入 RUNNING。 |
| 01:39:18.748140378 | 客户端 9092 接收线程记录 `Awaiting socket connections`。 |
| 01:39:18.754350143 | Broker 从 STARTING 进入 STARTED。 |
| 01:39:35 | Pod 的 `Ready=True` 条件最后转换时间；来自 Pod 状态快照，而非 Kafka 日志。 |

Controller 与客户端接收线程对应的两条日志相隔约 **11.47 秒**，说明本次真实启动确有分阶段就绪的时间差。本部署的 Startup 和 Readiness 都对 Controller 9093 做 TCP 检查，Liveness 执行 `pgrep -f kafka`，见 Pod 状态快照。TCP 探针成功只说明目标端口可以建立连接，不是 Kafka Produce/Fetch 协议检查。

## PAT 反例与真实日志的关系

PAT 检查 `[] (KafkaReady -> ClientEnableCheckpointReached)`，并给出：

```text
<init -> b_process_start -> b_controller_bound -> b_broker_socket_bound -> b_metadata_loaded -> b_unfenced -> sp_invoke -> sp_tcp_connected -> sp_success -> lp_invoke -> lp_exit_zero -> lp_success -> k_commit_lp_success -> rp_invoke -> rp_tcp_connected -> rp_success -> k_commit_rp_success>
```

这条**模型轨迹**在 Kubelet 提交 Readiness 成功后令 `KafkaReady` 成立，而客户端处理启用前的抽象检查点尚未到达，因此后续的 `b_client_enable` 也未发生。它没有发送 Produce/Fetch 请求。`ClientEnableCheckpointReached` 是验证用的抽象边界，不是 Kafka 源码中的独立操作。

需要特别区分时间：PAT 轨迹把三种探针都排在 `b_unfenced` 后；本次日志从解除 unfence 到 9092 接收线程就绪只有约 **8 毫秒**。无时间模型允许这种事件交错，归档**不能证明**真实 Kubelet 按此轨迹执行。Pod 的 `Ready=True` 最后转换时间晚于 9092 接收线程就绪；归档中也没有逐次探针成功或 Kafka 容器 Ready 转换的历史时间戳。因此当前结论是：**源码和实机日志支持“Controller 先于客户端处理就绪”的前提，PAT 给出候选误报 Ready 的反例，但本次启动没有证据证明 Pod 曾在客户端处理启用前变为 Ready，更没有业务请求失败证据。**

## 复核入口

模型入口为 [ReadinessGapLTL.csp](../ReadinessGapLTL.csp)，通过相对包含关系使用 [KafkaBroker.csp](../KafkaBroker.csp)、[Kubelet.csp](../../Kubelet/Kubelet.csp)及两个原生探针文件。PAT 命令格式：`PAT3.Console.exe -csp ReadinessGapLTL.csp gap-counterexample.txt`；从模型所在目录运行。更完整的源码映射和限制见 [Broker 设计文档](../design.md)。
