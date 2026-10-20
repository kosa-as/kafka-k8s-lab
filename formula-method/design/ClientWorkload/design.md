# ClientWorkload：真实 Java 客户端的 M0 行为模型

## 固定对象与源码依据

本角色抽象仓库现有的 [TimestampProducer.java](../../../clients/producer/src/main/java/com/example/kafka/producer/TimestampProducer.java) 和 [TimestampConsumer.java](../../../clients/consumer/src/main/java/com/example/kafka/consumer/TimestampConsumer.java)，并接入单目标 [KafkaBroker.csp](../KafkaBroker/KafkaBroker.csp)。生产者源码设置 `acks=all` 与幂等生产，调用异步 `producer.send(..., callback)`，最后执行 `flush()`；消费者订阅 topic，在循环中执行 `consumer.poll(Duration.ofMillis(250))`，对返回的每条记录写日志。客户端模块的 Maven 配置使用 `kafka-clients 4.3.1`，目标 Broker 镜像报告 Kafka `4.0.0`；本版建模的是本仓库客户端代码可观察到的调用与结果，尚未逐一证明两个版本的所有协议分支。

模型针对 `demo` topic 中**选中并路由到 `bitnami-kafka-controller-0` 的一次操作**。实际客户端先连 `bootstrap.servers` 获取元数据，再按分区 Leader 路由；生产者可能连续发送多个请求，消费者也可能同时运行。这里选择一个请求、一次只允许一个在途操作，以便与 Broker M0 模型的状态上限一致。后续组合已由 [ClusterContext](../ClusterContext/design.md) 对 `demo` 分区 2 的 Leader、ISR、副本确认和目标记录可用性施加约束；消费者分配及其他分区仍未展开。

## 源码行为与事件

| Java 客户端行为 | CSP# 事件 | 本版结果含义 |
| --- | --- | --- |
| `producer.send(new ProducerRecord(...), callback)` | `cw_send_produce` | 客户端发起异步发送；**尚不表示目标 Broker 已接收**。 |
| Producer 请求到达目标 Broker | `client_produce` | 与 Broker 的 NetworkHandler 和 RequestHandler 同步。 |
| Broker 返回 Produce 成功响应 | `b_response_produce_ok → cw_produce_success` | 成功回调代表这次写入获得所需确认。 |
| Broker 返回可重试错误 | `b_response_produce_error → cw_produce_retry`，或 `cw_delivery_expired → cw_produce_error` | 单次 Broker 错误不直接推出应用层失败；错误回调表示模型选择了交付期限到达或终止错误分支。 |
| `consumer.poll(250ms)` | `cw_begin_poll` | 客户端开始一次轮询；本版只抽取其中面向目标 Broker 的一个 Fetch。 |
| Fetch 到达目标 Broker | `client_fetch` | 与 Broker 请求路径同步。 |
| Fetch 返回数据/空结果，轮询随后返回 | `b_response_fetch_data/empty → cw_poll_data/empty` | `cw_poll_data` 表示取得记录；`cw_poll_empty` 仅表示本次轮询返回，没有取得业务数据。 |
| 目标 Broker 不再是分区 Leader，返回 Fetch 协议错误 | `b_response_fetch_error → cw_fetch_retry` | 客户端内部继续处理这次 Poll；此事件不表示 Java `poll()` 抛异常，也不表示已成功改连远端 Leader。 |

核心进程位于 [ClientWorkload.csp](ClientWorkload.csp)。`cw_kind` 表示当前选择 Produce 或 Fetch；`cw_pending=1` 表示本次操作尚未形成应用层结果，即使 Broker 已发出响应、客户端尚未完成后续处理，它也仍为 1。因此这个变量单独不能证明 Broker 尚未处理请求。`cw_last_result` 区分 Produce 成功、Produce 错误、Fetch 有数据和 Fetch 空结果；`cw_fetch_retry_seen` 只记录 Fetch 协议错误后的内部重试阶段。每次新操作都会清除上次结果，操作结束后经 `cw_next` 才能开始下一次。

当目标 Broker 在等待期间发生 `container_restart`，模型将该操作重新放回等待目标 Broker 接收的阶段。这是为了保留“客户端操作仍未结束”的抽象，**不保证真实 Java 客户端必然重试成功**：实际 Kafka 客户端可能重试、改连别的 Broker，或在自身交付期限后报告错误。模型只给出重试与最终结束的压缩分支，具体重试次数、元数据刷新和超时计时尚未展开。已收到 Broker 成功响应但尚未执行本地回调时，即使发生重启，模型仍保留该回调结果。

Kafka 4.0 [Producer 配置](https://kafka.apache.org/40/generated/producer_config.html)允许对暂时性错误重试，默认 `delivery.timeout.ms` 为 120 秒；仓库 Producer 没有覆盖该值。当前模型用 `cw_produce_retry` 与 `cw_delivery_expired` 区分重试和最终失败，但不计时，因而**不能从一次 60 秒网络隔离推断真实 Producer 一定触发错误回调**。

## 组合验证

[ClientWorkloadVerify.csp](ClientWorkloadVerify.csp) 对独立客户端角色检查 **13 条性质，全部 VALID**：无死锁、两类在途操作、四种应用层结果与 Fetch 内部重试阶段可达，以及调用后保持等待状态、结果后结束等待状态。[独立验证记录](verification.txt)保存 PAT 输出。独立可达性只说明客户端角色允许这些分支，并不表示 Broker 一定能返回。

[ClientWorkloadIntegrated.csp](ClientWorkloadIntegrated.csp) 构造两层组合：

- `ClientBrokerSystem() = KafkaBroker() || ClientWorkload()`：13 条新增断言全部 VALID，检查无死锁、四种应用层组合结果与 Fetch 内部重试阶段可达，以及请求抵达和 Broker 响应时客户端仍在等待对应操作。
- `ClientProbeSystem() = System() || KafkaBroker() || ClientWorkload()`：7 条新增断言全部 VALID，检查完整组合无死锁、四种应用层结果与 Fetch 内部重试阶段可达，以及 `KafkaReady`、客户端等待 Produce 结果与当前 Broker 尚未启用这三个状态可以同时存在。

该文件还因包含 `Kubelet.csp` 而重复执行其原有 18 条 `System()` 断言；[组合验证记录](integrated-verification.txt)因此共显示 **38 条 VALID**，其中只有 20 条是本文件针对新组合声明的。独立与组合合计新增 33 条断言声明。

只包含 Broker 与客户端时，`container_restart` 仍是未由 Kubelet 限制的外部事件，因此该组合的可达轨迹可能出现自发重启。只有 `ClientProbeSystem()` 才包含已有 Kubelet 的失败阈值与重启控制；两层组合的轨迹不能混作同一实际启动过程。

`CWReadyWithPendingProduce` 是**状态共现检查**：`cw_pending=1` 表示客户端回调尚未结束，不能单凭它判断请求是否已经送达或被 Broker 处理；重启后也可能继续保持此状态。该性质既不要求客户端在 Ready 之后才发送，也不包含实际时间或请求截止期限。真实客户端可能因交付超时而以错误结束操作，不能推断它会无限等待。该可达性不能单独证明真实业务失败；前一阶段归档的 Ready 候选反例仍应按 [证据归档](../KafkaBroker/ReadinessGapArchive-2026-10-20/README.md)中的限制解释。

## 当前边界与后续接口

本版保留了客户端“发起操作”和 Broker“收到请求”之间的区别，但尚未建模完整的 bootstrap 元数据交互、分区选择、批量与并发发送、幂等序列号、消费者再均衡、自动提交 offset、内部重试细节及真实计时。因此不能从当前 `reaches` 结果推导“所有请求最终完成”。现有 [FaultEnvironment](../FaultEnvironment/design.md) 为网络、执行与存储条件加入抽象故障；[ClusterContext](../ClusterContext/design.md) 已在选定分区上约束 Leader、ISR、副本确认和数据到达。空轮询与副本等待仍需按各自语义解释。
