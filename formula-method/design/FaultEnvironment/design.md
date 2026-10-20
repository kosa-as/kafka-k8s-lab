# FaultEnvironment：单故障环境 M0 模型

## 角色范围与证据边界

[FaultEnvironment.csp](FaultEnvironment.csp) 控制此前 [KafkaBroker.csp](../KafkaBroker/KafkaBroker.csp) 中已经预留的探针路径、客户端路径、JVM 执行和存储推进条件。核心过程一次只激活一种故障，以限制状态空间；故障事件由环境选择，并非 Kafka 或 Kubelet 源码自动生成。目标仍是单个合一 Controller/Broker Pod，不把 Controller TCP 连通性、客户端业务路径和副本间网络视为同一条链路。

| 故障模式 | 模型条件与直接影响 | 依据及限制 |
| --- | --- | --- |
| 探针路径不可达 | `probe_path_ok=0`，Startup/Readiness 到 Controller 9093 的 TCP 检查不能成功 | 本部署的实际探针配置见 [快照](../Kubelet/live-probe-snapshot.json)；这是独立路径故障的抽象，没有对应的已执行故障注入。 |
| 客户端路径不可达 | `client_path_ok=0`，新 Produce/Fetch 不能抵达目标 Broker | 与探针路径分开，表示客户端到 Client listener 的故障；仓库当前 Chaos Mesh 示例没有直接注入这种故障。 |
| 局部 JVM 执行停滞 | `execution_ok=0`，`b_execute_produce/fetch` 不能推进；Kafka 进程及 `pgrep` 结果不因此自动改变 | 对应 [KafkaRequestHandler.run](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/KafkaRequestHandler.scala#L103-L165) 所在处理阶段的抽象故障，**不是已在实机制造或复现的线程死锁**。本版假定重启可以清除此局部停滞。 |
| 存储步骤停滞 | `storage_ok=0`，`b_append_done/b_read_done` 不能推进 | 作用点对应 Broker 存储步骤；仓库的 Chaos Mesh 说明明确说 IOChaos 可用性尚未验证，因此只作模型条件。默认容器重启不会修复底层存储故障。 |
| Broker 对端网络隔离 | `peer_link_ok=0`，表示节点 0 与节点 1、2 的 Pod 间链路中断 | 来源于仓库的 [NetworkChaos 分区示例](../../../chaos-mesh/examples/kafka-network-partition.yaml)，该清单仅作为文件存在，本轮**没有执行故障注入**。[ClusterContext](../ClusterContext/design.md) 已将此条件接到目标分区 ISR 和副本确认；Controller 法定人数尚未展开。 |

仓库另有 [200ms 网络延迟示例](../../../chaos-mesh/examples/kafka-network-delay.yaml) 和 [容器杀死示例](../../../chaos-mesh/examples/kafka-container-kill.yaml)。M0 没有时钟，不把 200ms 延迟伪装成即时故障；容器杀死也未接入当前 Kubelet 阈值控制，以免把外部 kill 错写成 Liveness 触发的重启。

## 状态转移与重启语义

`fe_mode=0` 表示无故障；模式 1–5 分别对应上表。每个注入事件只改变其对应的条件变量，恢复事件将其还原。一种故障未恢复前，不会再选择第二种故障。完整组合依次包含 Kubelet、Broker、[ClientWorkload](../ClientWorkload/ClientWorkload.csp) 与本角色。

容器重启时，探针路径、客户端路径、存储和对端网络故障保持原状态，直到环境恢复；局部执行停滞则在重启后清除。这是**故障来源假设**，不是由原生探针结果推出的恢复保证。客户端及 Broker 仍按各自已有的重启事件复位。

## PAT 验证

[FaultEnvironmentVerify.csp](FaultEnvironmentVerify.csp) 的 **12 条断言全部 VALID**：独立角色无死锁、五种故障各自可达，以及无故障/单故障模式不会意外改变其他四项条件。[独立记录](verification.txt)保存完整结果。

[FaultEnvironmentIntegrated.csp](FaultEnvironmentIntegrated.csp) 的 **11 条新增组合断言全部 VALID**：完整组合无死锁，正常 Produce 成功路径与五种故障仍可达，Ready 可与客户端路径、执行、存储或对端网络故障同时出现。[组合记录](integrated-verification.txt)共显示 **29 条 VALID**，其中另外 18 条是包含 `Kubelet.csp` 后重复执行的旧 `System()` 断言。这里的 Ready 与故障共现不表示请求已超时。

[ExecutionProbeWitness.csp](ExecutionProbeWitness.csp) 使用本角色的聚焦入口 `FEExecutionOnProduce()`：它在 `client_produce` 被接收后、`b_execute_produce` 之前注入局部执行停滞，并阻止该执行步骤抢先越过故障注入。验证观察者记录请求已接收，及故障期间连续两次 `lp_success`。[聚焦 PAT 记录](execution-probe-witness.txt)显示 `ExecutionProbeGap` **可达（VALID）**，同一入口另有 18 条被包含的 Kubelet 旧断言为 VALID。关键事件顺序为：

```text
cw_send_produce → client_produce → ew_accept → fe_exec_after_accept
→ lp_success → ew_live → lp_success → ew_live
→ rp_success → k_commit_rp_success
```

以上是 PAT 轨迹中的**关键子序列**，不是完整逐事件日志；完整路径见记录文件。到达的状态满足：Kafka 容器被标为 Ready、Produce 已到达目标 Broker、请求仍待结果、关键执行步骤被阻断，同时 Liveness 已在故障期间成功至少两次。它揭示的是 `pgrep` 与请求执行进度的**模型层观测差异**。故障是人为设定的持久局部执行停滞，模型没有真实时间、Kubelet 调度公平性或实机注入证据，故不能据此宣称部署中已经发生漏检。只有在论文中先把这种局部停滞明确界定为“重启有望恢复且应被 Liveness 发现”的目标故障，才能将其称为候选 Liveness 漏检。

[ExecutionLivenessGapLTL.csp](ExecutionLivenessGapLTL.csp) 进一步检查候选目标 `[] (lp_success -> !AcceptedExecutionStalled)`：如果已接收的 Produce 在 `b_execute_produce` 之前遭遇可由容器重启清除的局部执行停滞，Liveness 不应继续报告成功。[LTL 反例记录](execution-liveness-gap.txt)中这条性质 **NOT valid**；反例包含 `cw_send_produce → client_produce → ew_accept → fe_exec_after_accept → lp_success`，且该次请求未经过 `b_execute_produce`。这里 `cw_pending=1` 仅说明客户端回调尚未结束；“Broker 尚未执行 Produce”由聚焦入口阻断 `b_execute_produce`、以及 RequestHandler 必须先执行该步骤才能形成 Produce 响应共同保证，不能由 `cw_pending` 单独推出。同一记录里另外 19 次检查为 VALID（被包含的 18 条 Kubelet 旧断言和 1 条聚焦可达性断言）。此性质是论文选定故障目标下的**更强健康要求**，并非原生 `pgrep` 探针或 Kubernetes 的现有契约；反例说明模型能够揭示该目标与现有观测之间的差距，尚不构成实机故障复现。

## 下一步约束

`peer_link_ok` 仍不直接改写 Broker 结果；现有 [ClusterContext](../ClusterContext/design.md) 已将它接到目标分区的远端 Fetch 受阻、ISR 更新及副本确认条件。网络隔离不保证立即缩小 ISR，也不保证本地 Leader 始终不变。网络延迟仍需要时间或有界等待语义。若要将模型候选轨迹与真实系统对应，还需独立设计故障注入，并同步采集客户端结果、探针结果、Kafka 容器 Ready 及 Broker 分区指标。
