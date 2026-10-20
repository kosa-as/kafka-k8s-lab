# M0 模型已检查的性质

本清单对应当前六个带 `#assert` 的 CSP# 文件。共有 **49 条不同的断言声明**：ReadinessProbe 6 条、LivenessProbe 6 条、Kubelet 18 条、KafkaBroker 独立模型 9 条、Broker 与探针组合 9 条、候选 Ready Gap 1 条。所有断言前也有逐条注释。

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

## KafkaBroker 独立模型：9 条，均 VALID

文件：[KafkaBrokerVerify.csp](KafkaBroker/KafkaBrokerVerify.csp)。此处尚未与 Kubelet 组合；Produce/Fetch 响应的 `reaches` 只表示存在成功路径，外部副本条件仍可选择成功。

| 断言 | 含义 |
| --- | --- |
| `KafkaBroker() deadlockfree` | 健康基线下 Broker 抽象模型没有可达死锁。 |
| `KafkaBroker() reaches ClientEnableCheckpointReached` | 存在启动到客户端处理启用前检查点的路径。 |
| `BrokerObserved() reaches ProduceOKSeen` | 存在 Produce 成功响应路径。 |
| `BrokerObserved() reaches FetchDataSeen` | 存在 Fetch 返回数据路径。 |
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

## 计数与边界

六个 PAT 记录共执行 **85 次断言检查**：84 次 VALID，1 次预期的 NOT valid。85 次不等于 85 条不同性质：`KafkaBrokerProbe.csp` 和 `ReadinessGapLTL.csp` 都包含 `Kubelet.csp`，各自会把其中 18 条针对原始 `System()` 的断言再运行一遍；它们**不会因此自动变成**针对 `BrokerProbeSystem()` 或 `ReadinessGapSystem()` 的组合断言。

当前结果只适用于无时间、单在途请求、固定健康条件的 M0 抽象。尚未验证“所有请求最终完成”，也未引入执行、网络、存储停滞下的 Liveness 漏检性质；这些需要后续 FaultEnvironment、ClientWorkload 和 ClusterContext 进一步约束。
