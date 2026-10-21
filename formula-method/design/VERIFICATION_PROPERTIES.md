# M0 模型已检查的性质

> **历史基线（2026-10-20）**：本文及对应日志保留原貌供追溯。2026-10-21 整体优化后的当前入口见 [M0-R2](M0/README.md)，当前性质与结果见 [M0/VERIFICATION.md](M0/VERIFICATION.md)。下文的“当前”均指历史运行时点；其中副本确认和 ISR 收缩抽象已在新版修正。

本清单对应当前十五个带 `#assert` 的 CSP# 文件。共有 **133 条不同的断言声明**：ReadinessProbe 6 条、LivenessProbe 6 条、Kubelet 18 条、KafkaBroker 独立模型 11 条、Broker 与探针组合 9 条、候选 Ready Gap 1 条、ClientWorkload 独立模型 13 条、客户端组合模型 20 条、FaultEnvironment 独立模型 12 条、故障完整组合 11 条、聚焦执行故障 1 条、候选 Liveness Gap 1 条、ClusterContext 独立组合 11 条、七角色聚焦组合 10 条、候选副本 Ready Gap 3 条。各文件中的注释说明断言用途。

`deadlockfree` 检查模型是否存在无法继续的可达状态；`reaches P` 只检查**存在一条**到达 `P` 的路径；`[] P` 检查所有路径上始终满足 `P`；`X P` 表示下一个模型状态满足 `P`；`<> P` 表示最终满足 `P`。可达性为 VALID 不表示每次请求最终都完成。

## ReadinessProbe：6 条，均 VALID

文件：[ReadinessProbeVerify.csp](ReadinessProbe/ReadinessProbeVerify.csp)。这些只验证独立的 TCP Readiness 探针角色。

| 断言 | 含义 |
| --- | --- |
| `RPIdle() deadlockfree` | 探针模型没有可达死锁状态。 |
| `RPChecked() reaches RPSuccessSeen` | TCP 连接成功后，存在报告 Success 的路径。 |
| `RPChecked() reaches RPFailureSeen` | TCP 失败或超时后，存在报告 Failure 的路径。 |
| `[] (rp_invoke -> <> (rp_success || rp_failure || container_restart))` | 一次调用最终有结果，或被容器重启打断。 |
| `[] (rp_tcp_connected -> X (rp_success || container_restart))` | TCP 成功后下一步报告成功，或遇到重启。 |
| `[] ((rp_tcp_failed || rp_timeout) -> X (rp_failure || container_restart))` | TCP 失败或超时后下一步报告失败，或遇到重启。 |

## LivenessProbe：6 条，均 VALID

文件：[LivenessProbeVerify.csp](LivenessProbe/LivenessProbeVerify.csp)。它检查 `pgrep -f kafka` 的探针结果映射，不检查请求是否能完成。

| 断言 | 含义 |
| --- | --- |
| `LPIdle() deadlockfree` | 探针模型没有可达死锁状态。 |
| `LPChecked() reaches LPSuccessSeen` | 命令零退出后，存在报告 Success 的路径。 |
| `LPChecked() reaches LPFailureSeen` | 命令非零退出或超时后，存在报告 Failure 的路径。 |
| `[] (lp_invoke -> <> (lp_success || lp_failure || container_restart))` | 一次调用最终有结果，或被容器重启打断。 |
| `[] (lp_exit_zero -> X (lp_success || container_restart))` | 零退出后下一步报告成功，或遇到重启。 |
| `[] ((lp_exit_nonzero || lp_timeout) -> X (lp_failure || container_restart))` | 非零退出或超时后下一步报告失败，或遇到重启。 |

## Kubelet + 两类探针：18 条，均 VALID

文件：[Kubelet.csp](Kubelet/Kubelet.csp)，受检系统是 `System()`，其中 Startup 是 Kubelet 的内部阶段。

| 断言 | 含义 |
| --- | --- |
| `System() deadlockfree` | Kubelet 与探针组合没有可达死锁。 |
| `System() reaches Started` | 存在通过 Startup 门控的路径。 |
| `System() reaches KafkaReady` | 存在将此 Kafka 容器标为 Ready 的路径。 |
| `System() reaches StartupThreshold` | 存在累计到 30 次连续 Startup 失败的路径。 |
| `System() reaches LivenessThreshold` | 存在累计到 3 次连续 Liveness 失败的路径。 |
| `[] (rp_invoke -> Started)` | Startup 成功前不调用 Readiness。 |
| `[] (lp_invoke -> Started)` | Startup 成功前不调用 Liveness。 |
| `[] (KafkaReady -> Started)` | 此容器被标为 Ready 时 Startup 门已打开。 |
| `[] (startup_threshold -> StartupThreshold)` | 触发 Startup 阈值事件时计数确实为 30。 |
| `[] (liveness_threshold -> LivenessThreshold)` | 触发 Liveness 阈值事件时计数确实为 3。 |
| `[] (startup_threshold -> X container_restart)` | Startup 阈值事件后下一步重启。 |
| `[] (liveness_threshold -> X container_restart)` | Liveness 阈值事件后下一步重启。 |
| `[] (container_restart -> X k_reset)` | 重启事件后下一步执行 Kubelet 状态重置。 |
| `[] (k_reset -> X (!Started && !KafkaReady))` | 重置后 Startup 门关闭，此容器不再 Ready。 |
| `[] (rp_success -> X k_commit_rp_success)` | 原始 Readiness 成功后下一步提交结果。 |
| `[] (k_commit_rp_success -> X (KafkaReady && RPFailRunZero))` | 提交成功后设为 Ready，失败计数清零。 |
| `[] (rp_failure -> X k_commit_rp_failure)` | 原始 Readiness 失败后下一步提交结果。 |
| `[] (RPFailRunSix -> !KafkaReady)` | 连续 6 次 Readiness 失败后取消此容器的 Ready 状态。 |

## KafkaBroker 独立模型：11 条，均 VALID

文件：[KafkaBrokerVerify.csp](KafkaBroker/KafkaBrokerVerify.csp)。此处尚未与 Kubelet 组合；Produce/Fetch 响应的 `reaches` 只表示存在成功路径，外部副本条件仍可选择成功。

| 断言 | 含义 |
| --- | --- |
| `KafkaBroker() deadlockfree` | 健康基线下 Broker 抽象模型没有可达死锁。 |
| `KafkaBroker() reaches ClientEnableCheckpointReached` | 存在启动到客户端处理启用前检查点的路径。 |
| `BrokerObserved() reaches ProduceOKSeen` | 存在 Produce 成功响应路径。 |
| `BrokerObserved() reaches FetchDataSeen` | 存在 Fetch 返回数据路径。 |
| `BrokerObserved() reaches FetchErrorSeen` | 目标分区 Leader 不在本节点时，Fetch 错误响应路径可达。 |
| `BrokerObserved() reaches NotLeaderProduceSeen` | Produce 的非本地 Leader 错误分支可达。 |
| `[] (client_produce -> ClientEnableCheckpointReached)` | 接收 Produce 请求前已到达客户端处理启用前检查点。 |
| `[] (client_fetch -> ClientEnableCheckpointReached)` | 接收 Fetch 请求前已到达该检查点。 |
| `[] (b_controller_bound -> !ClientEnableCheckpointReached)` | Controller 端口绑定时尚未到达该检查点。 |
| `[] (b_response_produce_ok -> ClientEnableCheckpointReached)` | Produce 成功响应前已到达该检查点。 |
| `[] (b_response_fetch_data -> ClientEnableCheckpointReached)` | Fetch 数据响应前已到达该检查点。 |

`ClientEnableCheckpointReached` 表示模型已走到客户端请求处理启用前的观察边界，它早于 `b_client_enable`。未到达该边界可以确定启用事件尚未发生；已经到达则仍不能断言客户端请求可以完成。此边界是验证用的抽象，不是 Kafka 源码中的独立操作。因此相关断言只检查启用顺序的必要条件，不能代替业务请求完成性质。

## Broker + Kubelet + 探针：9 条，均 VALID

文件：[KafkaBrokerProbe.csp](KafkaBroker/KafkaBrokerProbe.csp)，受检系统是 `BrokerProbeSystem() = System() || KafkaBroker()`。

| 断言 | 含义 |
| --- | --- |
| `BrokerProbeSystem() deadlockfree` | 健康基线的组合模型没有可达死锁。 |
| `BrokerProbeSystem() reaches ClientEnableCheckpointReached` | 组合后仍有路径到达客户端处理启用前检查点。 |
| `BrokerProbeSystem() reaches ProbeReadyGap` | 存在 `KafkaReady && !ClientEnableCheckpointReached` 的启动状态。 |
| `[] (rp_invoke -> Started)` | 组合后 Readiness 仍受 Startup 门控。 |
| `[] (lp_invoke -> Started)` | 组合后 Liveness 仍受 Startup 门控。 |
| `[] (container_restart -> ProbeRestartCause)` | 组合中的重启来自 Startup 或 Liveness 失败阈值。 |
| `[] (k_reset -> X (!Started && !KafkaReady))` | 组合中的重置清除 Startup 和 Ready 状态。 |
| `[] (b_response_produce_ok -> ClientEnableCheckpointReached)` | 组合中的 Produce 成功响应要求已到达该检查点。 |
| `[] (b_response_fetch_data -> ClientEnableCheckpointReached)` | 组合中的 Fetch 数据响应要求已到达该检查点。 |

## 候选跨层 Ready 性质：1 条，预期 NOT valid

文件：[ReadinessGapLTL.csp](KafkaBroker/ReadinessGapLTL.csp)；受检系统是 `ReadinessGapSystem() = System() || KafkaBroker()`。

| 断言 | 含义与结果 |
| --- | --- |
| `[] (KafkaReady -> ClientEnableCheckpointReached)` | 候选性质：“凡是 Ready，至少已到达客户端处理启用前检查点”。PAT 给出启动期间 Ready 已成立、但连该检查点都未到达的反例，故 NOT valid；此时 `b_client_enable` 必然尚未发生。实际轨迹见 [gap-counterexample.txt](KafkaBroker/gap-counterexample.txt)。 |

## ClientWorkload 独立模型：13 条，均 VALID

文件：[ClientWorkloadVerify.csp](ClientWorkload/ClientWorkloadVerify.csp)，受检系统是 `ClientWorkload()`。四种应用层结果的可达性只覆盖客户端角色分支；Fetch 协议错误后的内部重试另行检查。

| 断言 | 含义 |
| --- | --- |
| `ClientWorkload() deadlockfree` | 客户端抽象角色没有可达死锁。 |
| `ClientWorkload() reaches CWProducePending` | 可发起一个等待中的 Produce 操作。 |
| `ClientWorkload() reaches CWFetchPending` | 可发起一个等待中的 Fetch/Poll 操作。 |
| `ClientWorkload() reaches CWProduceSuccess` | Producer 成功回调分支可达。 |
| `ClientWorkload() reaches CWProduceError` | Producer 在交付期限到达或终止错误后进入最终错误回调的分支可达；单次 Broker 错误不直接证明此结果。 |
| `ClientWorkload() reaches CWFetchData` | Poll 取得记录的分支可达。 |
| `ClientWorkload() reaches CWFetchEmpty` | Poll 返回空结果的分支可达；这不代表取得业务数据。 |
| `ClientWorkload() reaches CWFetchRetrySeen` | 收到 Fetch 协议错误后，客户端内部重试阶段可达；这不是应用层错误回调。 |
| `[] (cw_send_produce -> X CWProducePending)` | 发起异步发送后进入 Produce 等待状态。 |
| `[] (cw_begin_poll -> X CWFetchPending)` | 开始轮询后进入 Fetch 等待状态。 |
| `[] (cw_produce_success -> X (CWProduceSuccess && !CWProducePending))` | 成功回调后记录结果并结束本次 Produce 等待。 |
| `[] (cw_poll_empty -> X (CWFetchEmpty && !CWFetchPending))` | 空轮询后记录结果并结束本次 Fetch 等待。 |
| `[] (cw_fetch_retry -> X (CWFetchRetrySeen && CWFetchPending))` | 内部重试发生后，本次 Poll 仍在进行。 |

## ClientWorkload 与现有角色组合：20 条，均 VALID

文件：[ClientWorkloadIntegrated.csp](ClientWorkload/ClientWorkloadIntegrated.csp)。前 13 条针对 `ClientBrokerSystem()`，后 7 条针对含 Kubelet 与探针的 `ClientProbeSystem()`。

| 断言 | 含义 |
| --- | --- |
| `ClientBrokerSystem() deadlockfree` | 网络、执行、存储守卫为健康值时，客户端与 Broker 组合没有可达死锁；此组合的重启仍是外部事件。 |
| `ClientBrokerSystem() reaches CWProduceSuccess` | 组合中存在 Produce 成功回调路径。 |
| `ClientBrokerSystem() reaches CWProduceError` | 组合中存在 Produce 错误回调路径。 |
| `ClientBrokerSystem() reaches CWFetchData` | 组合中存在 Poll 取得记录的路径。 |
| `ClientBrokerSystem() reaches CWFetchEmpty` | 组合中存在 Poll 返回空结果的路径。 |
| `ClientBrokerSystem() reaches CWFetchRetrySeen` | Broker 返回 Fetch 协议错误后，客户端内部重试阶段可达。 |
| `[] (client_produce -> CWProducePending)` | Produce 请求抵达 Broker 时，客户端有对应的等待操作。 |
| `[] (client_fetch -> CWFetchPending)` | Fetch 请求抵达 Broker 时，客户端有对应的等待操作。 |
| `[] (b_response_produce_ok -> CWProducePending)` | Broker 发出 Produce 成功响应时，客户端仍在等待该操作。 |
| `[] (b_response_produce_error -> CWProducePending)` | Broker 发出 Produce 错误响应时，客户端仍在等待该操作。 |
| `[] (b_response_fetch_data -> CWFetchPending)` | Broker 发出 Fetch 数据响应时，客户端仍在等待该操作。 |
| `[] (b_response_fetch_empty -> CWFetchPending)` | Broker 发出 Fetch 空响应时，客户端仍在等待该操作。 |
| `[] (b_response_fetch_error -> CWFetchPending)` | Broker 发出 Fetch 错误响应时，客户端操作仍未形成应用层结果。 |
| `ClientProbeSystem() deadlockfree` | 客户端、Broker、Kubelet 和探针的健康基线组合没有可达死锁。 |
| `ClientProbeSystem() reaches CWProduceSuccess` | 完整组合中 Produce 成功回调路径仍可达。 |
| `ClientProbeSystem() reaches CWProduceError` | 完整组合中 Produce 错误回调路径仍可达。 |
| `ClientProbeSystem() reaches CWFetchData` | 完整组合中 Poll 取得记录路径仍可达。 |
| `ClientProbeSystem() reaches CWFetchEmpty` | 完整组合中 Poll 空结果路径仍可达。 |
| `ClientProbeSystem() reaches CWFetchRetrySeen` | 完整组合中 Fetch 协议错误后的内部重试阶段仍可达。 |
| `ClientProbeSystem() reaches CWReadyWithPendingProduce` | Ready、客户端仍待 Produce 结果与当前 Broker 尚未启用可同时存在；不能据此判断请求是否已送达，也不是实机故障证据。 |

## FaultEnvironment 独立模型：12 条，均 VALID

文件：[FaultEnvironmentVerify.csp](FaultEnvironment/FaultEnvironmentVerify.csp)。五类故障一次只激活一种，其他四项条件保持健康。

| 断言 | 含义 |
| --- | --- |
| `FaultEnvironment() deadlockfree` | 故障角色没有可达死锁。 |
| `FaultEnvironment() reaches FEProbePathDown` | 探针路径故障可达。 |
| `FaultEnvironment() reaches FEClientPathDown` | 客户端路径故障可达。 |
| `FaultEnvironment() reaches FEExecutionHung` | 局部执行停滞可达。 |
| `FaultEnvironment() reaches FEStorageStalled` | 存储停滞可达。 |
| `FaultEnvironment() reaches FEPeerPartitioned` | Broker 对端网络隔离可达。 |
| `[] (FENormal -> FEHealthyAll)` | 无故障模式下全部条件健康。 |
| `[] (FEProbePathDown -> FEProbeIsolated)` | 探针路径故障不直接改变其他条件。 |
| `[] (FEClientPathDown -> FEClientIsolated)` | 客户端路径故障不直接改变其他条件。 |
| `[] (FEExecutionHung -> FEExecutionIsolated)` | 执行停滞不直接改变网络和存储条件。 |
| `[] (FEStorageStalled -> FEStorageIsolated)` | 存储停滞不直接改变网络和执行条件。 |
| `[] (FEPeerPartitioned -> FEPeerIsolated)` | 对端隔离不直接改变本 Pod 探针、客户端、执行和存储条件。 |

## 故障环境与现有角色组合：11 条，均 VALID

文件：[FaultEnvironmentIntegrated.csp](FaultEnvironment/FaultEnvironmentIntegrated.csp)，受检系统为 `FaultSystem()`。

| 断言 | 含义 |
| --- | --- |
| `FaultSystem() deadlockfree` | 含故障的组合模型没有可达全局死锁；客户端请求仍可能停滞。 |
| `FaultSystem() reaches CWProduceSuccess` | 可选择无故障路径并成功完成 Produce。 |
| `FaultSystem() reaches FEProbePathDown` | 组合中探针路径故障可达。 |
| `FaultSystem() reaches FEClientPathDown` | 组合中客户端路径故障可达。 |
| `FaultSystem() reaches FEExecutionHung` | 组合中执行停滞可达。 |
| `FaultSystem() reaches FEStorageStalled` | 组合中存储停滞可达。 |
| `FaultSystem() reaches FEPeerPartitioned` | 组合中对端网络隔离可达。 |
| `FaultSystem() reaches FEReadyClientPathDown` | Ready 与客户端路径中断可同时存在。 |
| `FaultSystem() reaches FEReadyExecPending` | Ready、等待 Produce 结果和执行停滞可同时存在。 |
| `FaultSystem() reaches FEReadyStoragePending` | Ready、等待 Produce 结果和存储停滞可同时存在。 |
| `FaultSystem() reaches FEReadyPeerPartition` | Ready 与对端网络隔离可同时存在；`ClusterContext` 尚未消耗此条件。 |

## 聚焦执行故障：1 条，VALID

文件：[ExecutionProbeWitness.csp](FaultEnvironment/ExecutionProbeWitness.csp)。`FEExecutionOnProduce()` 在 Broker 接收 Produce 后、执行步骤前注入局部停滞；观察者只在故障期间计数 Liveness 成功。

| 断言 | 含义 |
| --- | --- |
| `ExecutionWitnessSystem() reaches ExecutionProbeGap` | 可到达“Ready、Produce 已接收但未执行、仍待结果、执行条件阻塞，且故障期间两次 Liveness 成功”的状态。这是无时间模型中的候选观测差异，不是实机漏检证明。 |

## 候选 Liveness Gap 性质：1 条，预期 NOT valid

文件：[ExecutionLivenessGapLTL.csp](FaultEnvironment/ExecutionLivenessGapLTL.csp)；受检系统仍是 `ExecutionWitnessSystem()`。本文件包含聚焦见证文件，所以另有 19 条旧断言被重复执行。

| 断言 | 含义与结果 |
| --- | --- |
| `[] (lp_success -> !AcceptedExecutionStalled)` | 候选目标：“已接收的 Produce 在关键执行步骤前因局部停滞无法推进时，Liveness 不继续成功”。PAT 给出 `client_produce → ew_accept → fe_exec_after_accept → lp_success` 的反例，故 NOT valid。`cw_pending` 只表示客户端操作尚未形成最终结果，不能单独证明 Broker 未处理；执行步骤被阻断由聚焦故障入口保证。该目标比原生 `pgrep` 探针更强，不代表 Kubernetes 规范被违反，也不是实机复现。完整反例见 [execution-liveness-gap.txt](FaultEnvironment/execution-liveness-gap.txt)。 |

## ClusterContext 与故障环境：11 条，均 VALID

文件：[ClusterContextVerify.csp](ClusterContext/ClusterContextVerify.csp)。初始对象是只读查询确认的 `demo` 分区 2、Leader 0、ISR 0/1/2；网络隔离与 ISR 更新是分开的事件。

| 断言 | 含义 |
| --- | --- |
| `CCFaultSystem() deadlockfree` | 分区环境与故障环境组合没有可达全局死锁。 |
| `CCFaultSystem() reaches CCPartitionUnderMin` | 隔离后远端副本逐步离开 ISR，最终低于最低 ISR 的状态可达。 |
| `CCFaultSystem() reaches CCPartitionLeaderAway` | 隔离后目标分区 Leader 迁离节点 0 的分支可达。 |
| `CCFaultSystem() reaches CCRecoveredMin` | 链路恢复后至少一个远端副本重新进入 ISR 的分支可达。 |
| `[] CCIsrBound` | ISR 计数保持在压缩模型的合法范围。 |
| `[] (cc_follower_ack -> CCPeerLinkHealthy)` | 对端链路断开时不生成新的远端确认。 |
| `[] (b_isr_sufficient -> CCWriteCapable)` | 成功的 ISR 检查要求本地 Leader 且 ISR 不少于 2。 |
| `[] (b_isr_insufficient -> CCUnderMinIsr)` | ISR 不足检查只在本地 Leader 的 ISR 低于 2 时发生。 |
| `[] (b_not_leader_produce -> CCRemoteLeader)` | 非本地 Leader 错误要求目标分区 Leader 已迁离节点 0。 |
| `[] (b_replicas_ack -> CCWriteCapable)` | 所需副本确认时仍满足本地 Leader 与最低 ISR 条件。 |
| `[] (b_fetch_data_ready -> CCDataAvailable)` | Fetch 数据就绪时已有目标记录。 |

## 七角色对端隔离聚焦组合：10 条，均 VALID

文件：[ClusterContextIntegrated.csp](ClusterContext/ClusterContextIntegrated.csp)。`ClusterSystem()` 组合两个探针、Kubelet、Broker、客户端、故障环境和分区环境；故障环境只保留对端隔离/恢复，探针只选一次成功 Startup 与 Readiness 后冻结。这是轨迹子集，不用于全局死锁或活性结论。

| 断言 | 含义 |
| --- | --- |
| `ClusterSystem() reaches CWProduceSuccess` | 正常分区条件下 Produce 成功回调可达。 |
| `ClusterSystem() reaches CWFetchData` | Fetch 取得目标记录可达。 |
| `ClusterSystem() reaches CWFetchEmpty` | 无目标记录时 Fetch 空结果可达。 |
| `ClusterSystem() reaches CWFetchRetrySeen` | 非本地 Leader Fetch 协议错误后的客户端内部重试阶段可达。 |
| `ClusterSystem() reaches CCReadyUnderMin` | Ready 与目标分区 ISR 低于 2 可以共现。 |
| `ClusterSystem() reaches CCReadyUnderMinBrokerReject` | Ready、ISR 不足与 Broker 发出 Produce 协议错误可以共现；客户端仍可能重试。 |
| `ClusterSystem() reaches CCReadyRemoteLeader` | Ready 与目标分区 Leader 已迁离节点 0 可以共现。 |
| `[] (b_isr_sufficient -> CCWriteCapable)` | 完整组合中 ISR 检查成功仍要求本地 Leader 与最低 ISR。 |
| `[] (b_replicas_ack -> CCWriteCapable)` | 完整组合中成功确认仍要求本地 Leader 与最低 ISR。 |
| `[] (b_fetch_data_ready -> CCDataAvailable)` | 完整组合中 Fetch 数据就绪仍有目标记录。 |

## 聚焦副本 Ready 空窗：3 条，2 条 VALID、1 条预期 NOT valid

文件：[ReadinessReplicaGapLTL.csp](ClusterContext/ReadinessReplicaGapLTL.csp)。`CCIsrScenario()` 先让 Ready 成立，再施加对端隔离，并固定本地 Leader，以检查一条具体可能路径。它包含上面的组合文件，所以额外重复运行 28 条旧断言。

| 断言 | 含义与结果 |
| --- | --- |
| `CCIsrScenario() reaches CCReadyUnderMin` | 两个远端副本的 Fetch 受阻、ISR 逐步缩至 1 后，Ready 仍为真；VALID。 |
| `CCIsrScenario() reaches CCReadyUnderMinBrokerReject` | 上述状态下新 Produce 经 ISR 检查收到 Broker 协议错误，客户端仍在处理这次发送；VALID。 |
| `[] ((KafkaReady && CCLocalLeader) -> CCWriteCapable)` | 候选业务目标：“本地目标分区 Leader 显示 Ready 时，满足 `acks=all` 的最低 ISR 条件”。PAT 给出 Ready 后网络隔离及 ISR 收缩的反例，故 NOT valid。它强于原生 Controller TCP 探针，不是 Kafka/Kubernetes 实现违规。见[完整记录](ClusterContext/readiness-replica-gap.txt)。 |

## 计数与边界

十五个 PAT 记录共执行 **288 次断言检查**：285 次 VALID，3 次预期的 NOT valid。288 次不等于 288 条不同性质：包含 `Kubelet.csp` 的文件会重复执行其中 18 条针对原始 `System()` 的断言；它们**不会因此自动变成**针对其他组合系统的断言。`ExecutionLivenessGapLTL.csp` 重复执行聚焦执行故障的可达性断言，`ReadinessReplicaGapLTL.csp` 还重复执行七角色组合的 10 条断言。

当前结果只适用于无时间、单目标分区、单在途请求及聚焦故障选择的 M0 模型。尚未验证“所有请求最终完成”、真实探针周期内的漏检或网络隔离必然引起的 ISR 变化；故障持续时间、Leader 实际转移与实机因果关系仍需额外约束或实验。
