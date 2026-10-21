# 源码审计与旧模型修正

基线提交 `5447c72bf58cb53853f0295cf5625f1d081775c8`。本次先审查抽象正确性，再寻找跨层契约反例。固定源码及 SHA-256 见 [sources.json](evidence/sources.json)；仓库客户端为 **Kafka clients 4.3.1**，Broker 为 **Kafka 4.0.0**，两者分别引用对应版本。

## 源码到状态转移

| 源码位置 | 对应抽象 | 本版处理 |
|---|---|---|
| [Partition.scala](evidence/Partition.scala):1354–1380 `appendRecordsToLeader` | 本地 Leader、minISR 检查与追加在同一读锁区间 | `b_io_begin → b_io_complete` 持锁，禁止 ISR/Leader 写入穿越 |
| 同文件:1090–1125 `checkEnoughReplicasReachOffset` | 先检查 HW，再用当前 maximal ISR 判断成功/AFTER_APPEND | HW 与响应判定分步；错误不等同于没有追加/提交 |
| 同文件:1153–1190 `maybeIncrementLeaderHW` | under-min-ISR 不推进 HW；按各副本进度取约束 | 两个远端各自 ACK，提交检查全部当前 ISR |
| 同文件:1232–1293 `maybeShrinkIsr/isFollowerOutOfSync` | 写锁更新；LEO 相同的副本不按落后移除 | lag 需要追加导致的差距；另有空闲负对照 |
| 同文件:1428–1463 `fetchRecords` | 本地读取与 Leader 校验受读锁保护 | 读完成前不提前产生非 Leader 响应 |
| [DelayedProduce.scala](evidence/DelayedProduce.scala):84–125 | 后续检查、请求过期、结果回调 | `bp=4 → response → b_deliver` 分离 |
| [BrokerServer.scala](evidence/BrokerServer.scala) `startup/enableRequestProcessing` | 元数据追赶与 unfence 后启用客户端处理 | 冷启动阶段 `life=0..6`；Controller TCP 可较早开放，继承旧启动源码映射 |
| [Sender.java](evidence/Sender.java):186–216、362–376、428–433 | 排队和在途批次均可超时，不要求先收到错误 | 客户端期限与 Broker 处理独立 |
| [ProducerConfig.java](evidence/ProducerConfig.java) `DELIVERY_TIMEOUT_MS_DOC` | 交付期限包含排队、等待确认、重试 | 期限不在协议重试后重新计时 |
| [KafkaConsumer.java](evidence/KafkaConsumer.java):859–876 | poll 到期可返回空集合 | Poll 空结果不表示 Broker 没有目标数据；假设无阻塞用户回调 |
| [worker.go](evidence/worker.go):367–390 | 未达阈值保留原结果，达阈值更新 resultsManager | 失败计数与 Ready 缓存，不把一次失败立即映射为 NotReady |

源码 URL 分别是 `apache/kafka` 的 `4.0.0` / `4.3.1` 标签以及 `kubernetes/kubernetes` 的 `v1.36.1` 标签。下载保留原文件及许可证头；内容哈希固定了此次实际阅读的文本。模型源码映射不是部署镜像逐字节认证。

部署条件继续使用 [Kubelet 快照](../Kubelet/live-probe-snapshot.json)、[分区快照](../ClusterContext/live-topic-snapshot.txt) 与仓库 `bitnami/helm-values.yaml`。Startup/Readiness 为 Controller 9093 TCP，Liveness 为 `pgrep -f kafka`，阈值 30/6/3，RF=3、minISR=2、Producer `acks=all`。

## 修正的建模缺陷

| 编号 | 旧模型问题 | 后果 | 新版修正/核查 |
|---|---|---|---|
| A01 | ISR=3 时一个远端 ACK 布尔值即可确认 | 允许不符合 acks=all 的过早成功 | 两副本进度；H02/H03/S01；ACK 守卫变异检测 |
| A02 | `b_isr_sufficient` 与 `b_append_request` 分离，中间可缩 ISR | 产生源码锁语义不允许的追加路径 | 显式 I/O 持锁阶段；S04/M02 |
| A03 | 仅断网即允许 ISR 逐次缩减 | 空闲、已追平副本被错误移出 | 追加差距+lag+完成 ISR 更新；I01/I02 |
| A04 | 只有收到 Broker 错误之后才能交付超时 | 执行/磁盘/链路挂起时漏掉客户端终态 | 独立期限；D03/P01/F07 |
| A05 | Broker 响应与客户端收到合为同一事件，发送没有网络守卫 | 漏掉提交后丢失响应和迟到响应 | `response/rx/client` 三阶段；N02/N03/N06 |
| A06 | 聚焦副本场景冻结探针并固定 Leader | 持续 Ready 的结论部分由场景人为造成 | 新 Probe 循环；ISR 场景限定复制路径；元数据迁移单列；R01/M01 |
| A07 | Fetch 读取中途提前非 Leader 返回，Storage 还在等完成 | 可能遗留无人同步的读完成事件 | 读锁期间不迁移；没有独立悬空 Storage 进程状态 |
| A08 | Core 中夹带 System 的旧断言，被组合入口重复继承 | 执行条数夸大整体覆盖，混淆受检系统 | role/system/check 分离；每条断言只针对当前 `M0()` 或其被动观察组合 |
| A09 | ACK epoch 在追加完成时才用当前 Leader 重新赋值 | 旧请求可能跨迁出/迁回被重新授权 | 本版明确只保留一次迁出，不模拟重新当选；未声称证明任意世代安全 |

上述是**旧模型的缺陷或覆盖不足**，不能作为 Kafka/Kubernetes 的实现缺陷写入论文。新的安全检查和负对照用于验证本次修正保留了必要约束；它们不替代模型与实现的形式化精化证明。
