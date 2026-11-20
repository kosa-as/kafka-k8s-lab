# health-sidecar 设计方案与 M1 一致性审查

## 论文研究范围对齐（2026-11-10）

本设计的研究问题与输出边界以 [最新第一、二章约束](../../docs/THESIS_CONSTRAINTS.md) 为准：交付健康观测、事件标签、状态判定及探针映射；流量调整与重启由平台执行，本文不以处置策略或其修复效果作为贡献。以下采集动作是实现候选及待核验条件，不能由设计文本推定为已实现。连续计数形成标签，sidecar 状态、单次探针结果与 Pod Ready 分开记录；对执行、存储、ISR、UNKNOWN 的具体规则继续以当前 M1 和原始结果为证据。第五章比较检测表现并记录业务指标，其因果解释需控制平台处置与实际路由。

本文以 [M1 CSP 模型](sidecar-csp/model/System.csp) 为准，给出 Pod 内 sidecar 的实现契约。当前仓库只有 CSP/PAT 模型，没有实现该检测器；[现有 Helm values](../../bitnami/helm-values.yaml) 的 `health-sidecar` 只周期性写入固定的 `status=healthy`。因此“符合模型”在本文中指实现必须满足的条件，**不表示现有容器已经满足**。

## 1. 审查结论

前版方案的总体方向可用，但有两处与当前 M1 不一致，已在下文修正：

1. M1 的 [LivenessProbe](sidecar-csp/model/LivenessProbe.csp) 在有效观测连续 `N` 次确认执行路径失败后，下一次 LP 采样就返回失败；模型没有“JVM 死锁等额外强证据”这一门。若实现加入额外门槛，E02 的 M1 验证结果不能直接用于该实现。
2. M1 的 [ReadinessProbe](sidecar-csp/model/ReadinessProbe.csp) 把原生 Kafka 条件和 sidecar 条件**合成为同一次 RP 采样**，然后由 Kubelet 累计 RP 失败。仅给 sidecar 容器另配一个 Readiness 探针，虽然也可让 Pod 退出 Ready，但容器级阈值、交错与 M1 不同。下文以 Kafka 容器上的组合探针为基准接线。

另有三个需由实现核验的观测假设：本地执行请求能代表 `exec_ok`，本地追加检查能代表所选 `disk_ok`，ISR 查询能获得及时的目标分区状态。当前 M1 按项记录有效性；ISR 查询缺失不屏蔽已取得的执行失败证据。每项证据的身份、阶段和时效仍需第五章分别核验。Sidecar 进程死亡、快照过期、容器异步重启及多分区聚合仍属后续扩展。

## 2. 作用范围与 Pod 接线

模型跟踪一个本地 Broker 和一个目标分区。实验可选 `demo` 的一个分区，RF=3、`min.insync.replicas=2`；模型使用 `local_leader==1 && isr1+isr2>=1` 表示本地 Leader 加至少一个远端 ISR 副本满足最低要求。若本地不是目标分区 Leader，ISR 检查视为通过。实现如果改为“任一分区不足即摘除整个 Pod”，属于新的聚合策略，需要另建模。

建议的部署接线为：

```text
Kafka 容器                         health-sidecar 容器
  Startup: 原启动检查                 周期性采集本地执行、存储、目标 ISR
  Readiness: 原生条件 AND sidecar 状态 ← 原子发布的本代快照
  Liveness: 原进程条件 AND 已确认执行失败的否定 ← 同一快照
                      │
                      ▼
                 Kubelet 的 30/6/3 阈值
```

Kafka 容器的 Readiness 和 Liveness 可以使用能读取 sidecar 快照的 `exec` 包装器；也可用可达的本地接口，但必须确保**探针配置在 Kafka 容器上**。Liveness 若配置在 sidecar 容器，失败只会重启 sidecar，不能对应 M1 的 Kafka 重启。组合 Readiness 的一次返回值应同时检查原 Kafka 条件、`sc_started` 和 `sc_ready`；不能将两个容器独立的 Readiness 阈值直接等同于 M1 的一个 `rp_fail` 计数器。实际 Chart 32.5.0 的模板及镜像内命令能力须用 `helm template` 与容器检查确认，本文不声称已有 values 完成接线。

sidecar 可提供 `/status` 用于实验记录，但探针热路径宜读取同一份**原子发布**的状态快照，避免读取半写入状态。快照至少包含 Kafka 启动代际、采样代际、有效性、三项观测、三个失败计数、`sc_started`、`sc_ready`、更新时间和失败原因。代际防止重启后沿用旧判定。快照存储或 HTTP 只是一种实现载体，不改变模型的状态定义。

## 3. 观测如何对应 [Observation.csp](sidecar-csp/model/Observation.csp)

一次采集按 `sc_begin → 结果提交 → sc_decide` 进行。每项使用 `sc_*_valid` 标记有效性；`sc_valid` 仅表示三项是否完整，不决定其他项的有效证据能否使用。请求已送达目标且采集器正常时，有依据的超时可记为该项有效失败；身份或阶段不明记为该项 UNKNOWN。全部不可取得时三项均为 UNKNOWN。Sidecar 不读取故障标签，所有采集仍需超时与本地身份约束。

执行观测与 ISR 观测分别取得证据：可以通过其他 Broker 或独立元数据快照查询目标分区，但必须验证身份映射与时效。如果 ISR 查询失败而执行失败证据有效，提交执行有效失败与 ISR UNKNOWN，执行计数仍独立累计。当前分项规则与检查见 [PER_METRIC.md](sidecar-csp/PER_METRIC.md)。

| M1 结果 | 模型中成功条件 | 候选实现证据与限定 |
|---|---|---|
| `sc_exec` | `life==6 && exec_ok==1` | 对本 Pod Broker 的**请求处理路径**发出本地请求并收到完整、有效的响应。TCP connect 和可被其他 Broker 代答的客户端请求均不足以作为该证据。B02 的冷启动还要求此请求只在模型的请求处理启用阶段后成功。 |
| `sc_disk` | `sc_exec==1 && disk_ok==1` | 先确认执行路径正常，再完成受控的**本地目标日志追加**。专用健康分区写入是候选，但必须确认该 Broker 是该分区 Leader、请求确实在本地完成，并记录它只能覆盖所选日志路径；`acks=1` 成功不能证明物理刷盘或全部业务分区健康。 |
| `sc_isr` | 非本地 Leader，或两个远端 ISR 中至少一个仍在 ISR | 读取目标分区当前 Leader、ISR 及有效 `min.insync.replicas`。在本实验 RF=3、minISR=2 的范围内按本地 Leader 加远端 ISR 计算。元数据陈旧、身份不明时 ISR 项记为未知，不将未知误判为 ISR 充足。 |

现有 Kafka Admin API、Producer 或 JMX 是否足以满足“本地、目标路径、及时完成”三项约束，尚需以 Kafka 4.0.0 实验确认。若无法满足，可以增加仅供本 Pod 使用的 Kafka server 侧诊断接口，并让 sidecar 负责调用与判定。新增接口必须先说明其请求实际经过何种处理线程及日志阶段，再用实验建立它与上述抽象状态的对应关系。

### 3.1 本地执行路径：用主动请求判定“停滞”

**每轮动作。**sidecar 直接连接本 Pod 的 Kafka 客户端监听地址，发送一个带唯一关联 ID 的轻量 Kafka Metadata 请求，等待完整且匹配的响应，设置硬超时 `T_exec`。必须禁用“失败后自动换其他 Broker”的探测行为；成功响应还要校验本地 Broker 身份。不能使用只证明端口开放的 TCP connect，也不以 ApiVersions 响应单独证明请求处理已经启用。Kafka 协议允许 Broker 回答元数据请求，但“它一定经过本文关注的所有处理线程”不是协议承诺，需通过注入实验验证。[Kafka 协议说明](https://kafka.apache.org/40/design/protocol/)

**分类规则。**请求在 `T_exec` 内完成，且本地身份、Broker 请求处理启用标记均正确，记有效 `sc_exec=1`；请求已发送到已确认的本地 Broker，但在期限内没有完整响应，或服务端能报告该请求一直没有完成，记有效 `sc_exec=0`。连接目标不确定、sidecar 自身不能调度、身份或启动标记不可读，记执行项 UNKNOWN。`sc_exec=0` 连续两轮才判 EXEC_STALL；单次超时或 Kafka 进程存在不构成已确认停滞。这里“停滞”指**选定本地请求处理路径在期限内无进展**，不等同于已定位 JVM 死锁。请求发出后的网络黑洞也可能表现为超时，单靠 sidecar 无法区分；第五章须专门检验误判，若要增加额外证据门槛，则先修改 M1 的 LP 规则再验证。JMX 的请求队列长度和处理线程空闲比例可记录为辅助证据，不能替代主动请求的完成结果。[Kafka 监控指标](https://kafka.apache.org/40/operations/monitoring/)

**B02 的附加条件。**M1 把 `sc_exec=1` 约束为 `life==6`，因此实验必须确认上述请求不会在 Broker 请求处理正式启用前成功。如果 Metadata 请求提前可答复，就需增加 Kafka server 侧的“请求处理已启用”只读标记，并把两项同时成功才算 `sc_exec=1`。该标记是拟增加的实现接口，现有仓库没有它。

### 3.2 本地存储路径：用受控追加判定“无进展”

**每轮动作。**只有本轮 `sc_exec=1` 才做独立存储归因。sidecar 对一个专用健康分区发送带本轮 nonce 的小消息，等待 `acks=1` 的成功响应，设置硬超时 `T_disk`。探测前后确认本 Pod Broker 仍为该分区 Leader；请求须定向发给本地 Broker，不允许客户端透明重定向到其他 Leader。Kafka 对 `acks=1` 的说明是 Leader 写入本地日志后返回响应，不等待所有 Follower。[Kafka Producer 配置](https://kafka.apache.org/40/generated/producer_config.html)

**分类规则。**本地 Leader 身份稳定且收到成功响应，记有效 `sc_disk=1`；执行探测仍成功、本地 Leader 身份稳定、追加请求已到达相应处理阶段却超过 `T_disk` 未完成，记有效 `sc_disk=0`。要证明“已到达追加阶段”，需要 server 侧阶段进展信号；没有该信号时，单纯 Produce 超时不能准确归因为存储停滞，应记 UNKNOWN。`NOT_LEADER`、配额、权限、Topic 缺失、元数据陈旧、客户端未能发出请求等情形也记 UNKNOWN。若 `sc_exec=0`，模型令 `sc_disk=0`，但 [Sidecar.csp](sidecar-csp/model/Sidecar.csp) 会中断未确认的存储失败计数，并保留既有的存储确认状态，同时独立累计执行异常。两轮有效的 `sc_exec=1 && sc_disk=0` 才判 STORAGE_STALL。

**覆盖范围。**建议在第五章先把健康分区与目标分区放在同一日志目录，并用 Kafka 的日志目录查询核对放置；这仍只能证明所选追加路径有进展。健康分区可能与业务分区遇到不同的分区锁或日志条件，`acks=1` 也不证明物理 `fsync` 完成。若论文要声称“目标分区存储路径”而非“选定本地追加路径”，需让 Kafka server 侧暴露目标分区的追加阶段进展，或把探测直接作用于目标分区，并修订模型中的 `disk_ok` 定义。[Kafka Admin 日志目录接口](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/admin/Admin.html)

### 3.3 ISR：逐分区比较 Leader、ISR 和有效门限

sidecar 从集群元数据通道查询**指定目标分区**的 Leader 与 ISR，并读取该 Topic 的有效 `min.insync.replicas`；对本实验固定的 minISR=2，亦应在启动时核验实际配置。若 `leader.id != 本 Pod broker.id`，模型中的 `sc_isr=1`；若是本地 Leader，则计算 `|ISR| >= min.insync.replicas`，本实验等价于 `isr1+isr2>=1`。连续两轮有效查询得到不足才判 ISR_UNDER_MIN。Kafka Admin 接口提供 Topic 描述和配置读取能力。[Kafka Admin API](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/admin/Admin.html)、[Topic 配置](https://kafka.apache.org/40/generated/topic_config.html)

每轮必须固定 Topic、partition、broker.id、配置版本或采集时间，拒绝把缓存的旧 ISR 当作新一轮成功结果。Leader、ISR 或配置读取失败，或者两次读取期间发生不一致的迁移，记 UNKNOWN。查询可经其他健康 Broker 完成，使本地请求处理停滞时仍可能取得 ISR 信息；但集群返回的元数据是否足够及时，需在第五章测量。当前 M1 仅跟踪一个目标分区，不能据此声称整个 Broker 的所有分区健康。

### 3.4 一轮观测的提交顺序

候选实现可并行发起本地执行请求与 ISR 查询，执行成功后再进行追加探测。到期后提交一份包含三个结果及对应有效位的原子快照；某项缺失不阻止其他有效结果进入判定。ISR 查询失败时，已确认目标身份与请求阶段的执行超时仍可提交为有效 `sc_exec=0`。观测周期、请求期限和元数据时效上限需记录在实验配置中；模型验证轮次与交错，不给出秒级性能结论。

## 4. 判定状态机：逐项对应 [Sidecar.csp](sidecar-csp/model/Sidecar.csp)

默认参数 `N=2`、`R=2`。它们是**连续观测次数**，不是秒数。一次 `sc_decide` 的规则如下：

1. 执行项有效成功时设置 `sc_started=1` 并清零执行失败计数；有效失败时累计至 `N`，与其他项是否缺失无关。
2. 执行有效成功且存储有效失败时，存储失败计数加一；两项均有效成功时解除存储异常。执行失败、执行未知或存储未知时，仅清零尚未达到 `N` 的存储计数，已确认异常保留。
3. ISR 项有效失败时独立累计，有效成功时清零；未知时中断未确认计数，保留已确认异常。执行项未知同样只中断未确认的执行计数。
4. 三项均有效且都为1时累计恢复，连续 `R` 轮后 `sc_ready=1`；任一失败或缺失清零恢复计数。任何已确认故障均撤销就绪。
5. 三项分别累计 `sc_*_unknown`，有效结果清零对应缺失计数。任何一项连续缺失达到 `N` 后撤销就绪；未知不单独新增 LP 失败，也不清除其他项或本项已确认的异常。

冷启动时 `sc_started=0, sc_ready=0`。启动检查仍由原 Kubelet 过程处理；即使启动探针先通过，组合 Readiness 也要等本地执行探测成功且连续 `R` 次三项健康观测后才能成功。稳定入口在模型中带有一次既有的成功观测，用来分析故障后的行为；真实实现启动时应从冷状态进入。

## 5. 探针与处置：逐项对应 M1

模型 [ReadinessProbe](sidecar-csp/model/ReadinessProbe.csp) 在 `started==1`、无未消费 RP 结果且 Broker 未退出时采样。返回成功的条件是：

```text
life >= 2 && probe_net == 1 && sc_started == 1 && sc_ready == 1
```

模型 [LivenessProbe](sidecar-csp/model/LivenessProbe.csp) 在同样的邮箱约束下采样；Broker 进程处于模型的运行阶段，且**并非** `sc_exec_fail==N` 时成功。存储失败、ISR 不足、UNKNOWN 均不会独立使 LP 失败。探针只读取已完成的 sidecar 判定，不能在 HTTP/exec 处理器中临时跑一次检测并跳过 `N` 次计数。

[Kubelet.csp](sidecar-csp/model/Kubelet.csp) 将采样结果与消费动作分开：Startup 失败阈值 30、Readiness 失败阈值 6、Liveness 失败阈值 3。Kafka 重启时模型原子清空 sidecar 状态、探针邮箱及计数。真实 sidecar 与 Kafka 是两个独立容器，故必须以 Kafka 启动代际使旧快照失效；这一同步尚未由现有 M1 证明。

对 sidecar 自身失效，建议 Kafka Readiness 保守失败；Kafka Liveness 仅在读到**本代、有效、已确认执行失败**的快照时按 M1 返回失败，其他情况回退到原进程检查，避免 sidecar 故障造成 Kafka 重启。此回退是**实现期安全扩展**：当前 M1 只含采集缺失 `LOSS`，未含 sidecar 容器死亡或快照过期。在将其写成已验证机制前，应增加相应 CSP 状态和性质。另可给 sidecar 容器自身设置 Liveness，使其只重启自身。

## 6. 可声称的验证结论与实施验收

现有 PAT 结果支持：B02 原公式在 M1 为 VALID；D02 的“已确认存储异常后下一次 RP 失败”在完整 M1 为 VALID；E02 与 R04 对应响应性质在聚焦模型为 VALID，完整 M1 全空间检查尚未完成。旧 E02、D02、R04 即时公式仍为 NOT valid。详见 [模型结果](sidecar-csp/README.md)与 [E02/R04 聚焦验证](sidecar-csp/E02_R04_FOCUSED.md)。**这些结果以抽象观测准确、相应采样持续发生为前提，不能直接证明上述接口实现已经符合模型。**

实施时按“观测证据 → sidecar 快照 → 探针结果 → Kubelet 动作”逐段验收：

| 场景 | 必须观察到的链路 |
|---|---|
| 冷启动 B02 | 本地执行请求成功之前 Kafka Pod 不 Ready；满足连续健康观测后才可能 Ready。 |
| 执行停滞 E02 | 有效执行失败连续两次 → 后续 Kafka 容器 LP 失败 → 连续三次达到重启阈值；验证实际重启动作另需 Kubelet 调度证据。 |
| 存储停滞 D02 | 执行探测成功、目标追加失败连续两次 → 后续 RP 失败 → 六次失败后 Pod NotReady；不因该原因单独重启 Kafka。 |
| ISR 不足 R04 | 本地 Leader 的目标 ISR 连续两次不足 → 后续 RP 失败 → Pod NotReady；不因该原因单独重启 Kafka。 |
| 采集缺失与恢复 | 同一项连续两次 UNKNOWN 撤销 sidecar 就绪且不独立触发 LP；其他项的有效异常仍独立累计；三项连续两次健康后恢复 sidecar 就绪。 |

真实环境的采样间隔、超时、元数据新鲜度、探测写入开销、sidecar 故障及容器重启交错，应在第五章记录并验证；若它们改变上述转移规则，先修订 CSP 模型再援引形式化结果。
