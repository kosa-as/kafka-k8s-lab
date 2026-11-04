# health-sidecar：面向 B02、E02、D02、R04 的设计方案

日期：2026-11-04。本文是下一版机制的**设计**，不是已部署或已通过 PAT 的结果。实现改动限定在 Kafka Pod 内的 sidecar、探针配置和必要的服务端观测；业务 Producer/Consumer 不需要改造。现有 `bitnami/helm-values.yaml` 中的 `health-sidecar` 只写固定 `status=healthy`，不能直接承担以下判定。

## 1. 检测对象与论文契约

论文中的服务范围先限定为当前三节点 KRaft 集群上的 `demo` Topic：3 个分区、RF=3、`min.insync.replicas=2`，研究 `acks=all` 写入和目标 Broker 的请求处理。sidecar 只对**本 Pod Broker**负责的请求处理及其当前担任 Leader 的目标分区作就绪决策。其他 Topic、全部客户端网络路径和完整集群健康不在一个 Pod 的就绪契约之内。

四个目标分别定义为：

| 来源 | 原生信号缺口 | 拟补充的检测证据 | 期望动作 |
|---|---|---|---|
| B02：Ready 早于请求处理启用 | Controller TCP 已开放 | 本地客户端监听器上的完整 Kafka 请求与有效响应，确认目标 Broker 的处理路径已启用 | 启动期间 sidecar NotReady |
| E02：进程在但关键执行路径停滞 | `pgrep` 仍成功 | 本地轻量协议请求持续超时、处理队列/完成进展异常；强重启证据可由 JVM 死锁检测补充 | 先 NotReady；只有明确的本地可重启执行故障才建议 Kafka Liveness 失败 |
| D02：存储路径不推进 | Controller TCP 仍可达 | 本地受控写入请求持续无结果，辅以 I/O 相关指标；需要区分执行路径与本地追加 | 持续异常后 NotReady；不因底层存储故障直接重启 |
| R04：目标分区 ISR 不足 | 原探针不检查分区 | `demo` 中本 Broker 为 Leader 的分区 ISR 与最低 ISR 配置，辅以 Broker 的聚合指标 | 目标写入能力持续受限后 NotReady；不直接重启 |

这里的“修复”表示在规定观测和 Kubelet 响应期限内识别问题，并使对应范围的就绪输出改变。**B02 的启动契约可以继续使用原即时公式；E02/D02/R04 的原即时公式在周期采样系统中不能无条件成立。** 后三项需用带持续时间、有效观测及采样公平前提的性质验证。不得通过直接修改故障环境变量或绕过 Kubelet 把这些原式强行改成 VALID。

## 2. Pod 接线

```text
同一 Kafka Pod
 ├─ kafka：原 Broker、原 Startup Probe；保留进程型兜底检查
 ├─ jmx-exporter：已有 9404/metrics
 └─ health-sidecar：本地协议/元数据/指标采集 → 事件 → 状态 → /ready 与重启建议文件
         ├─ 自己的 Readiness Probe → 参与 Pod Ready
         ├─ 自己的 Liveness Probe → 只重启 sidecar
         └─ 共享 emptyDir 中的带代际状态文件 → Kafka 容器的 Liveness exec 按严格条件读取
```

sidecar 的 Readiness 失败会使 Pod 的容器就绪条件不满足，因此无需 sidecar 直接修改 Pod `status` 或取得 Kubernetes API 写权限。保留 Kafka 原有 Readiness，可把 sidecar 作为附加门；最终 Pod Ready 需要两者均满足。sidecar 自身 Liveness 只负责重启 sidecar，**不会重启 Kafka 容器**。若要让增强判断触发 Kafka 重启，必须把读取重启建议的 Liveness Probe 配到 `kafka` 容器。Kubernetes 的探针动作按探针所属容器执行；Readiness 失败影响 Pod 的服务端点。[Kubernetes Pod 条件](https://kubernetes.io/docs/concepts/workloads/pods/pod-condition/)、[探针语义](https://kubernetes.io/docs/concepts/workloads/pods/probes/)。

sidecar 对外仅暴露三个简单结果：`/ready`（只表达当前约定服务是否可承接）、`/healthz`（sidecar 自身是否仍在运行）、`/status`（原因、观测时间和证据，供实验记录）。实际地址与端口在部署阶段固定，避免与现有 9404 指标端口冲突。若用 HTTP 探针，服务需监听 Pod IP 可到达的地址。

`kafka` 容器的 Liveness 建议通过 `exec` 读取共享 `emptyDir` 中的状态文件。文件含 Kafka PID/启动代际、sidecar 更新时间、`restart_recommended` 和原因码，采用原子替换写入。只有**本代、未过期、明确建议重启**才返回失败；sidecar 不可用或文件过期时沿用进程检查并把“判断未知”交给 sidecar Readiness，不让 sidecar 故障制造 Kafka 重启循环。Kafka 重启后旧文件必须失效。

现有 Chart 32.5.0 对 sidecar、`controller` 探针和卷挂载的具体 values 模板需要在部署前 `helm template` 核对。本文不把示意结构直接视为可应用的 Helm YAML。

## 3. 四类观测如何获得

### 3.1 B02：启动门控

sidecar 在本 Pod 的客户端监听器上发起**完整请求—响应**探测，不能只执行 TCP connect。候选是本地 Kafka 协议请求；必须在实际 4.0.0 镜像上核对该请求最早何时能够得到有效响应，并与 Broker 请求处理启用事件对齐。仅 `ApiVersions` 或普通 AdminClient 的集群级成功结果，可能覆盖不到论文定义的全部目标路径；不能未经验证就把它们等同于 `Enabled`。启动门控通过后仍需检查采样新鲜度，Kafka 容器重启时重新进入 STARTING。

### 3.2 E02：执行路径持续无响应

sidecar 周期性向本地 Broker 发送低成本协议请求，记录开始、成功、超时和连续失败。结合 JMX 请求队列、请求处理线程空闲比例，以及请求/响应累计进展，要求“有待处理工作或主动探测请求”时才累积停滞证据，避免正常空闲误报。Kafka 4.0 官方列有 `RequestQueueSize` 和 `RequestHandlerAvgIdlePercent` 等指标，但是否被仓库当前 JMX Exporter 规则完整导出，需要读取实际 `/metrics` 核对。[Kafka 4.0 监控指标](https://kafka.apache.org/40/operations/monitoring/)

`ThreadMXBean.findDeadlockedThreads()` 可作为**特定 JVM 锁死**的强辅助证据；它覆盖监视器和 ownable synchronizer 的死锁环，不覆盖所有线程停顿或 I/O 等待，且可能有运行开销。若不增加 Kafka 内部埋点，sidecar 无法凭外部请求保证观察到每一条 Kafka 关键线程的推进。[Java ThreadMXBean](https://docs.oracle.com/en/java/javase/24/docs/api/java.management/java/lang/management/ThreadMXBean.html)

执行停滞先降低 Readiness。Liveness 只对已确认、持续、与本地可重启故障相符的状态返回失败；普通请求超时、CPU 过载或采样缺失不直接触发重启。

### 3.3 D02：本地存储进展

为区分“Broker 能回答轻量协议请求”与“本地日志追加能推进”，可由 sidecar 向**受控探测 Topic**写少量消息。该探测分区在实验前确认由本 Broker 担任 Leader，设置单副本和 `acks=1`，使一次成功响应依赖本地 Leader 的日志写入，而不等待远端 ISR。Kafka 文档说明 `acks=1` 在 Leader 写入本地日志后响应，不等待全部 Follower。[Kafka Producer 配置](https://kafka.apache.org/40/generated/producer_config.html)

此探测只证明**所选本地追加路径**有进展，不能证明物理 `fsync`、所有日志目录或 `demo` 每个分区均可写。若研究目标明确为物理磁盘 Hang，应另加相同 PVC 上受控文件 I/O 完成观察，或增加服务端日志追加/刷盘阶段埋点；两者均须验证其对目标故障的覆盖与额外开销。

探测 Topic 需要限制消息频率与保留量；若 Leader 已迁移、Topic 不存在、元数据过期或采样侧自身异常，结果记为 UNKNOWN，不直接归因为磁盘故障。轻量协议请求也失败时优先记录“执行/请求路径异常”，避免把同一个失败重复归因为存储。

### 3.4 R04：目标分区 ISR

sidecar 通过 Admin API 读取 `demo` 每个分区的 Leader 与 ISR，并读取实际 `min.insync.replicas` 配置；仅对本 Broker 当前担任 Leader 的分区比较 `|ISR|` 与最低要求。`UnderMinIsrPartitionCount` 可作为快速聚合信号，但它不提供本文需要的目标分区和 Pod 级动作范围。[Kafka Admin API](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/admin/Admin.html)、[Kafka 监控指标](https://kafka.apache.org/40/operations/monitoring/)

这项检查的就绪语义必须写死为“**本 Pod 对纳入研究的目标分区，是否具备 `acks=all` 写入条件**”。在本地实验中可采用“任一受监测的本地 Leader 分区持续低于最低 ISR → sidecar NotReady”。此举会影响**整个 Pod** 的服务端点，可能连仍可读或其他健康分区的流量一起摘除；第五章必须测量这一代价。若将来研究多 Topic、多业务职责，宜输出分区级告警或独立服务路由，不能把本实验策略推广成通用 Broker 健康定义。

## 4. 事件、状态与动作规则

sidecar 为每种观测保存采样时间与有效性，按窗口计数，不让单次失败直接触发动作：

| 事件 | 触发证据 | 解除证据 | Readiness | Kafka Liveness |
|---|---|---|---|---|
| STARTING | 本地请求处理路径尚未通过完整探测 | 启动门控成功 | 失败 | 原 Startup/进程门控 |
| EXEC_STALL | 主动轻量请求持续未完成，并有处理无进展证据 | 请求与处理进展持续恢复 | 失败 | 仅有额外强本地重启证据时失败 |
| STORAGE_STALL | 轻量请求仍可完成、本地写入探测持续不完成，并有有效存储观测 | 本地追加探测持续恢复 | 失败 | 保持成功，不把底层 I/O 故障直接变成重启 |
| ISR_UNDER_MIN | 本地 Leader 的目标分区连续低于配置最低 ISR | ISR 恢复并稳定 | 对本文指定服务职责失败 | 保持成功 |
| UNKNOWN | 采样失效、元数据陈旧或探测路径无法归因 | 取得足够新鲜观测 | 经过短暂宽限后失败 | 保持进程型兜底，不因未知而重启 |

同一周期可能有多个事件：保留所有原因码，Pod Readiness 由需要摘流的事件合取决定，Liveness 只看明确的本地可重启证据。恢复需连续成功窗口，参数由第五章基线时延和故障实验确定；第四章 CSP 用有限的 N、R、U 表达规则，再做参数敏感性检查。sidecar 与 Kafka 的启动代际改变时，旧事件和计数全部失效。

## 5. CSP 模型如何对应

在现有 M0/M1 七角色组合外，将 sidecar 明确拆为 `LocalProtocolProbe`、`StorageProbe`、`ReplicaObserver`、`EventClassifier` 和 `SidecarDecision`；为控制状态空间，模型中可把一次采集与事件更新压缩为原子步骤，但**采样、探针返回和 Kubelet 消费必须保持可交错**。每个采集过程只能读取允许的观测投影，不能读取 `fault`、`exec_ok`、`disk_ok`、`replica_net` 等真实故障标签。

与 M0 的比较先固定故障场景和业务角色，再检查：

1. **B02 原公式** `[](Ready -> Enabled)` 在增强模型 VALID；对应的 B01 坏路径不可达。
2. 在持续 E/D 故障、有效观测和采样公平前提下，最终 `SidecarNotReady`，继而在探针消费公平前提下最终 `PodNotReady`；原生对照不保证相同结果。
3. 对 R04，在目标分区 ISR 持续不足、元数据新鲜且处于本文定义的服务范围内，最终 PodNotReady；不要求故障发生瞬间的旧 `[](Ready -> MinIsrSatisfied)` 无条件成立。
4. `STORAGE_STALL`、`ISR_UNDER_MIN` 或 `UNKNOWN` 单独存在时不触发 Kafka 重启；确认的可重启执行故障才可能触发 Liveness 失败。
5. 正常空闲、短暂抖动、探测 Topic Leader 迁移和 sidecar 自身失效不能直接形成“磁盘故障”或“应重启 Kafka”的结论。
6. 原有提交、安全、客户端终态、ISR 收缩条件等正确性性质继续回归。`reaches` 为 VALID 的坏路径见证如果被修复，应相应变成 NOT valid；不能机械要求所有旧 `reaches` 结果不变。

为避免上版 M1 的健康误报，正常运行模型必须**显式给出工作进展相对采样窗口的时间约束**。无约束交错下，健康 Broker 可以连续被采样而暂未获得执行机会；对此不能宣称零误报。模型中的窗口上界只能在声明的时序前提下成立，真实参数由第五章实验决定。

## 6. 分阶段落地与验收

**阶段一：旁路采集。** 替换现有固定状态文件 sidecar，先只记录本地协议、JMX、目标 ISR、探测 Topic 写入结果与观测新鲜度。核对所有目标信号的实际来源及 Broker 身份，保留原生探针。

**阶段二：接入 Readiness。** 先接 B02 启动门控，再分别接 E/D 持续停滞和目标 ISR 受限。记录 sidecar 判定时间、Kubelet 探针失败、Pod Ready 改变及业务请求结果，确认每个动作的范围。

**阶段三：审慎接入 Liveness。** 先只输出重启建议并收集“重启是否真的恢复”的对照数据；只有能识别明确本地可恢复故障且误重启可控时，才让 Kafka 容器的 Liveness 读取该建议。sidecar 自己的 Liveness 始终只负责 sidecar 进程。

验收时每类故障至少同时展示：故障注入确实生效、采集信号有效、事件与状态变化、Pod/Kafka 容器动作、业务结果以及恢复过程。形式化结果与实机结果分表报告；探测请求的资源消耗和误摘流也必须计入评价。
