# ClusterContext：目标分区、ISR 与副本确认的 M0 模型

## 固定对象与依据

本角色只跟踪 `demo` 的 **分区 2** 和当前单 Broker 模型对应的节点 0。2026-10-20 08:49:48 UTC 的[只读快照](live-topic-snapshot.txt)显示：分区 2 的 Leader 为 0，副本为 0、1、2，三者都在 ISR；本部署配置 `min.insync.replicas=2`，示例 Producer 设置 `acks=all`。快照是初始状态证据，不保证后续 Leader 或 ISR 不变。

Kafka 4.0 的[主题配置说明](https://kafka.apache.org/40/generated/topic_config.html)规定，`acks=all` 与最低 ISR 共同约束写入成功；ISR 不足可以产生 `NotEnoughReplicas` 或追加后的对应错误。[ReplicaManager.scala（4.0.0）](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ReplicaManager.scala)包含追加、延迟 Produce 与 ISR 到期检查流程。本模型把远端副本无法持续 Fetch、ISR 检查、追加后等待确认分成不同事件，并不把网络隔离瞬间等同于 ISR 缩小。

故障入口来自仓库现有的[双向网络隔离示例](../../../chaos-mesh/examples/kafka-network-partition.yaml)：节点 0 与节点 1、2 之间的 Pod 流量中断 60 秒。**本轮没有应用该清单，也没有测量隔离后的真实 ISR、Leader 或客户端响应。**

## 与六个已有角色的接口

[ClusterContext.csp](ClusterContext.csp) 读取 FaultEnvironment 的 `peer_link_ok`，并与 KafkaBroker 的纯事件同步：

| Broker 事件 | ClusterContext 的约束 |
| --- | --- |
| `b_isr_sufficient` / `b_isr_insufficient` | 目标分区仍由节点 0 领导，且 ISR 分别不少于 2 / 低于 2。 |
| `b_not_leader_produce` / `b_not_leader_fetch` | 目标分区 Leader 已迁离节点 0；Broker 返回错误。 |
| `b_append_request` / `b_append_done` | 只有本地 Leader 发起追加；完成本地追加后开始等待远端副本确认。 |
| `b_replicas_ack` / `b_replicas_timeout` | 成功要求本轮等待中已有远端确认、本地仍为 Leader 且 ISR 不少于 2；超时则走错误分支。 |
| `b_fetch_data_ready` / `b_fetch_timeout` | 本地读取取得目标记录，或在无目标记录时返回空结果。 |

为覆盖 Leader 迁离，Broker 的 RequestHandler 增加 Produce/Fetch 的非本地 Leader 错误分支。ClientWorkload 收到 Fetch 协议错误后保留本次 Poll，并进入内部重试的压缩阶段；这**不是** Java `poll()` 的应用层错误回调。Produce 的单次 Broker 错误也先进入客户端重试或交付期限分支，不直接等同于最终失败回调。单 Broker 模型只处理发往节点 0 的选中请求；Leader 迁离后的完整元数据刷新、改发远端 Broker 和真实交付期限不在本版范围内。

初始 `cc_local_leader=1`、`cc_isr=3`。对端链路断开后，`cc_follower_fetch_lost` 先记录一个远端副本 Fetch 受阻，后续 `cc_isr_shrink` 才把一个副本移出 ISR；两个远端副本分别经历这两个事件。若链路在收缩前恢复，`cc_follower_fetch_resumed` 清除这次待处理的受阻记录；已经收缩的 ISR 则通过 `cc_isr_expand` 表示副本重新追上并进入 ISR。另有独立的 `cc_leader_away`/`cc_leader_local_again` 元数据分支，避免假定节点 0 必然一直是 Leader。重启取消当前待确认请求，已提交数据不因重启消失。`cc_existing_record` 表示初始可能已有目标消费组尚未读取的记录。

这是无时间、单目标分区、单在途请求模型。`cc_follower_fetch_lost` 不模拟具体 Fetch 包或 `replica.lag.time.max.ms` 计时；`cc_isr_shrink` 表示到期检查之后已经观察到 ISR 更新，而不是保证网络隔离一定让 ISR 缩小。`b_replicas_timeout` 是有界等待到期的抽象选择，当前没有时间数值。

## PAT 验证与反例

[ClusterContextVerify.csp](ClusterContextVerify.csp) 的 **11 条断言全部 VALID**：隔离后 ISR 降至 1、Leader 迁离和恢复到最低 ISR 的状态可达；ISR 范围、远端确认、ISR 检查、Leader 错误及 Fetch 数据条件满足约束。[独立记录](verification.txt)保存完整结果。

[ClusterContextIntegrated.csp](ClusterContextIntegrated.csp) 把七个角色串联。为控制状态空间，它只保留 FaultEnvironment 的“对端隔离／恢复”分支，并在 Kubelet 中选取一次成功 Startup 与一次成功 Readiness，随后冻结新的探针调用。这是原模型允许的轨迹子集；**不用于证明全局无死锁或所有调度下的活性**。本文件新增的 **10 条断言全部 VALID**：正常 Produce、Fetch 数据/空结果及收到协议错误后的内部重试路径可达；Ready 可与目标分区 ISR 不足、Broker Produce 协议错误和 Leader 迁离共现；ISR、确认和 Fetch 数据事件满足局部约束。由于包含 Kubelet 文件，[组合记录](integrated-verification.txt)还重复运行了 18 条旧 `System()` 断言，共 28 条 VALID。

[ReadinessReplicaGapLTL.csp](ReadinessReplicaGapLTL.csp)进一步聚焦：先让 Ready 成立，之后隔离节点 0 与两个对端，并固定目标分区 Leader 仍在节点 0。PAT 的[场景记录](readiness-replica-gap.txt)显示两条可达性断言 VALID，其中一条的关键子序列为：

```text
k_commit_rp_success → fe_peer_partition
→ cc_follower_fetch_lost → cc_isr_shrink
→ cc_follower_fetch_lost → cc_isr_shrink
→ cw_send_produce → client_produce → b_execute_produce
→ b_isr_insufficient → b_response_produce_error → cc_note_produce_error
```

这是完整轨迹中的关键子序列；完整日志还包含启动和此前与本次故障无关的客户端操作。`cc_note_produce_error` 记录的是 Broker 发出的单次协议错误，此时客户端发送仍未完成；Kafka Producer 可继续重试。同一文件的候选业务性质 `[] ((KafkaReady && CCLocalLeader) -> CCWriteCapable)` 为 **NOT valid**：Ready 已成立、节点 0 仍为该分区 Leader，但 ISR 只有 1。该性质把“Ready”解释成“这个分区当下满足 `acks=all` 的最低写入条件”，**强于现有 Controller 9093 TCP Readiness 的语义**。Kafka 在 ISR 不足时返回错误符合其设计；反例揭示的是探针观测与所选业务可用性目标之间的差距，不是 Kafka 或 Kubernetes 的实现违规。

本场景把网络隔离安排在 Ready 后、固定 Leader 留在节点 0，属于一条可能执行路径而非集群必然路径。当前[故障示例](../../../chaos-mesh/examples/kafka-network-partition.yaml)只持续 60 秒，而 Kafka 4.0 Producer 默认[交付期限](https://kafka.apache.org/40/generated/producer_config.html)为 120 秒；不能由这里的 Broker 协议错误推出真实客户端一定报告最终失败。是否存在足够长的真实重叠窗口，还需要同步采集 Ready 转换、分区 Leader/ISR、Producer 重试及最终回调和实际故障时序。验证器没有在本轮对真实集群注入故障。
