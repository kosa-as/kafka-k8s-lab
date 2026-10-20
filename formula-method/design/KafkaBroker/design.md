# KafkaBroker：单 Pod 组合角色 M0 模型

## 固定对象与证据

目标是 `bitnami-kafka-controller-0` 中的 Kafka 容器，而不是完整三 Broker 集群。2026-10-20 从运行中的容器读取的 [配置快照](live-config-snapshot.json)记录了 Kafka `4.0.0`、镜像摘要、`process.roles=controller,broker`、Controller 监听端口 9093、Client 监听端口 9092、`min.insync.replicas=2` 和本地日志目录。模型对照 [Apache Kafka 4.0.0 源码](https://github.com/apache/kafka/tree/4.0.0)；部署镜像是 Bitnami 包装版，尚未逐文件核对镜像内是否有源代码补丁。客户端示例使用 `acks=all`，因此 Produce 成功响应还依赖副本条件。

模型分为 [KafkaBroker.csp](KafkaBroker.csp) 的五个内部进程，并通过 [KafkaBrokerProbe.csp](KafkaBrokerProbe.csp)接入现有 Kubelet、ReadinessProbe 和 LivenessProbe。独立断言在 [KafkaBrokerVerify.csp](KafkaBrokerVerify.csp)；所有已检查性质的逐项解释见 [M0 验证性质清单](../VERIFICATION_PROPERTIES.md)。

## 五个组件的源码对应

| 组件 | 源码与抽象行为 | 主要接口 |
| --- | --- | --- |
| `Lifecycle` | [`KafkaRaftServer.startup`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/KafkaRaftServer.scala#L75-L99)先启动 ControllerServer，再启动 BrokerServer；[`ControllerServer`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ControllerServer.scala#L160-L186)创建 Controller 端点；[`BrokerServer`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/BrokerServer.scala#L559-L612)等待元数据追赶和 unfence 后才启用客户端请求处理。 | `b_process_start`、`b_controller_bound`、`b_broker_socket_bound`、`b_metadata_loaded`、`b_unfenced`、`b_client_enable`、`lp_exit_zero/nonzero` |
| `NetworkHandler` | [`SocketServer`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/network/SocketServer.scala#L142-L175)在启动请求处理线程之前绑定端口；其 [Processor](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/network/SocketServer.scala#L1020-L1050)接收请求，结果经 [`RequestChannel`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/network/RequestChannel.scala#L378-L425)返回。Controller TCP 与 Client 请求路径分别建模。 | `sp_tcp_*`、`rp_tcp_*`、`client_produce/fetch`、`b_response_*` |
| `Execution` | [`KafkaRequestHandler.run`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/KafkaRequestHandler.scala#L103-L165)从请求队列取请求并调用 `KafkaApis.handle`。是否存在 `pgrep` 匹配进程与关键处理步骤是否推进保持独立。 | `b_execute_produce`、`b_execute_fetch` |
| `RequestHandler` | [`KafkaApis.handle`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/KafkaApis.scala#L149-L168)按 API 分派；Produce 调用 [`handleProduceAppend`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/KafkaApis.scala#L503-L519)，Fetch 调用 [`fetchMessages`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/KafkaApis.scala#L725-L741)。[`ReplicaManager`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/ReplicaManager.scala#L884-L909)可能延迟 Produce 响应，Fetch 也可能等待数据或超时。 | `b_isr_*`、`b_replicas_ack/timeout`、`b_fetch_data_ready/timeout` |
| `StorageAccess` | [`Partition.appendRecordsToLeader`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/cluster/Partition.scala#L1354-L1380)检查本地 Leader 与最小 ISR，再调用 [`UnifiedLog.appendAsLeader`](https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/log/UnifiedLog.scala#L700-L713)；Fetch 从本地日志读取。 | `b_append_request/done`、`b_read_request/done` |

## 关键顺序与探针接口

源码给出一个必须保留的先后关系：**Controller 端口建立 → Broker 客户端端口建立 → 元数据追赶及 unfence → 客户端请求处理启用**。`SocketServer` 的构造先绑定端口，`enableRequestProcessing` 后才启动相应处理。Controller 监听端口在组合角色中先于 Broker 的客户端处理路径建立。因此 `rp_tcp_connected` 只说明 Kubelet 到 Controller 端口的 TCP 连接成功，不能直接推出 Client 端口已处理 Produce/Fetch。

`Lifecycle`约束 Liveness `pgrep -f kafka` 的结果；`NetworkHandler`约束 Startup/Readiness 的 Controller TCP 结果。`probe_path_ok` 与 `client_path_ok` 是不同的外部路径条件，`execution_ok` 与 `storage_ok` 是另外两种推进条件；当前健康基线均为 1，后续 `FaultEnvironment` 才负责改变它们。这些条件不直接改写原生探针的判断规则。

请求路径使用一个在途请求的有限状态抽象：`client_produce/fetch → b_execute_* → b_append/read_request → b_append/read_done → 必要的副本或数据条件 → b_response_*`。Produce 在 `acks=all` 下分出 ISR 不足、所需确认到达、等待超时三种抽象结果；Fetch 区分返回数据与等待后空结果。`b_isr_*`、`b_replicas_*` 和 `b_fetch_*` 当前是未约束的外部条件入口，将由 `ClusterContext` 和 `ClientWorkload` 在后续模型中约束。空 Fetch 响应表示一次协议请求完成，不等于消费者取得业务数据。

## PAT 验证与候选反例

[独立验证记录](verification.txt)显示 `KafkaBroker()` 的 **9 条断言全部 VALID**，包括无死锁、Produce/Fetch 数据响应路径可达、以及这些事件发生时已到达客户端处理启用前检查点。[组合验证记录](probe-verification.txt)显示 **27 条断言全部 VALID**（其中 18 条来自已包含的 Kubelet 文件，9 条针对组合模型）；组合中的 `ProbeReadyGap = KafkaReady && !ClientEnableCheckpointReached` 可达。Broker 的进程同步还要求客户端请求在 `b_client_enable` 后才能被接收。

具体检验的是**启动期间探针认为 Kafka 容器已就绪，而客户端请求处理尚未启用**的场景：Controller 的 9093 端口可以接受 TCP 连接，Startup 和 Readiness 因而成功；与此同时，Broker 的 CLIENT 9092 请求处理仍未执行 `b_client_enable`。Liveness 的 `pgrep` 也可能成功，但它只表明进程匹配成功。此反例没有发送 Produce/Fetch 请求，也没有验证请求完成时间。

[ReadinessGapLTL.csp](ReadinessGapLTL.csp)检查 `[] (KafkaReady -> ClientEnableCheckpointReached)`。[PAT 反例记录](gap-counterexample.txt)将其判为 **NOT valid**。日志中的实际反例轨迹（省略起始标记 `<init>`）是：`b_process_start → b_controller_bound → b_broker_socket_bound → b_metadata_loaded → b_unfenced → sp_invoke → sp_tcp_connected → sp_success → lp_invoke → lp_exit_zero → lp_success → k_commit_lp_success → rp_invoke → rp_tcp_connected → rp_success → k_commit_rp_success`。轨迹结束时 Kubelet 已将 Kafka 容器标为 Ready，但模型仍未到达客户端处理启用前检查点，更未发生 `b_client_enable`。该检查点只是为了观察启动顺序而加入模型的边界，**到达检查点本身不表示已经能处理请求**；反例中的“尚未到达”则足以确定启用事件尚未发生。因此这是一项针对“Ready 先于启用”的必要条件检查：失败可以指出上述启动空窗，即使通过也不能证明 Ready 时请求一定可完成。

这是源码顺序和无时间调度允许的**候选观测空窗**，不表示 Bitnami Chart 或 Kubernetes 违反了自身的探针语义。2026-10-20 对当前 Pod 最近一次启动的只读核对显示：Kafka 日志在 **01:39:07.279 UTC** 记录 Controller 9093 的 `Awaiting socket connections`，在 **01:39:18.748 UTC** 才记录客户端 9092 的同类消息；两条日志相隔约 **11.47 秒**。Pod 状态中的 `Ready=True` 最后转换时间是 **01:39:35 UTC**，晚于客户端监听线程启动。这证明本次启动存在 Controller 与客户端端点先后就绪的时间差，**但没有证明该 Pod 在客户端请求处理启用之前就已变为 Ready**：现有 Kafka 日志没有逐次探针成功与 Kafka 容器 Ready 转换的时间戳，Pod Ready 还受其他容器影响。

还需注意：PAT 输出的**这一条具体反例**把 Startup、Liveness、Readiness 都排在 `b_unfenced` 之后；本次真实启动中 Broker 在 **01:39:18.740 UTC** 左右完成 unfence，客户端 9092 的接收线程约 **8 毫秒**后就开始等待连接。无时间模型允许在这一小段内插入全部探针事件，不能据此断言实际 Kubelet 曾按这条轨迹运行。较早的 Controller/客户端端点时间差说明存在可供探针调度的启动窗口，但是否真的导致误报 Ready，仍需同步采集探针结果、Kafka 容器 Ready 转换和客户端协议请求响应，并在模型中加入计时约束。相关原始日志行、Pod 状态和 PAT 反例已归入 [Ready 候选空窗证据归档](ReadinessGapArchive-2026-10-20/README.md)。

## 状态压缩与下一步

本版保留五个组件的并行同步，但每次只处理一个客户端请求；没有展开 Kafka 所有 API、每个分区、副本线程、磁盘页缓存或多个 Broker 的内部状态。Kubelet 原有模型仍一次只调度一个探针。`Execution` 和 `StorageAccess` 仅以能否推进作为接口，没有在没有故障来源的情况下凭空制造线程死锁或 I/O hang。Controller 端口、客户端请求和存储推进的独立性已经明确，下一步可用 `FaultEnvironment` 限制网络、执行、存储条件，再由 `ClusterContext` 提供目标分区 Leader/ISR 与副本确认约束。
