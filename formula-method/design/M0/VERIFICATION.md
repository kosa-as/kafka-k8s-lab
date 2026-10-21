# M0-R2 验证性质与实际结果

PAT 3.5.1；运行归档 UTC 时间 `2026-10-21T02:31:50.676630+00:00`。共 **14 个入口、43 条断言：34 VALID，9 NOT valid**。

S00 是七项安全条件的合取，只执行一次，并不重复计为七条。每个入口只包含当前组合的断言，无旧入口断言继承。

NOT valid 包括预先声明的健康契约候选、错误业务推断和一个不加公平性的调度负对照，不能把条数称作已确认源码缺陷数量。

`reaches` 的 VALID 只表示存在路径；LTL 的 VALID 表示在声明的有限模型及前提内未发现反例。结果解释见 [FINDINGS.md](FINDINGS.md)。

## 运行汇总

| 入口 | VALID / NOT valid | 最大访问状态数 | 入口墙钟秒 | 日志 |
|---|---:|---:|---:|---|
| [Healthy](checks/Healthy.csp) | 5 / 1 | 11,417 | 1.75 | [原始结果](results/Healthy.txt) |
| [WholeSafety](checks/WholeSafety.csp) | 1 / 0 | 2,710,020 | 160.109 | [原始结果](results/WholeSafety.txt) |
| [WholeProgress](checks/WholeProgress.csp) | 2 / 0 | 2,796,666 | 134.937 | [原始结果](results/WholeProgress.txt) |
| [Startup](checks/Startup.csp) | 2 / 1 | 1,950 | 0.547 | [原始结果](results/Startup.txt) |
| [ClientNetwork](checks/ClientNetwork.csp) | 5 / 1 | 112,553 | 6.625 | [原始结果](results/ClientNetwork.txt) |
| [Execution](checks/Execution.csp) | 1 / 1 | 117 | 0.531 | [原始结果](results/Execution.txt) |
| [Storage](checks/Storage.csp) | 2 / 1 | 277 | 0.532 | [原始结果](results/Storage.txt) |
| [Replica](checks/Replica.csp) | 3 / 2 | 121,760 | 6.812 | [原始结果](results/Replica.txt) |
| [IdleReplica](checks/IdleReplica.csp) | 2 / 0 | 193 | 0.484 | [原始结果](results/IdleReplica.txt) |
| [ProbeNetwork](checks/ProbeNetwork.csp) | 3 / 1 | 114,330 | 6.922 | [原始结果](results/ProbeNetwork.txt) |
| [FetchData](checks/FetchData.csp) | 2 / 0 | 657 | 0.485 | [原始结果](results/FetchData.txt) |
| [FetchEmpty](checks/FetchEmpty.csp) | 2 / 0 | 513 | 0.453 | [原始结果](results/FetchEmpty.txt) |
| [FetchNetwork](checks/FetchNetwork.csp) | 2 / 1 | 13,057 | 1.094 | [原始结果](results/FetchNetwork.txt) |
| [Metadata](checks/Metadata.csp) | 2 / 0 | 17,205 | 1.188 | [原始结果](results/Metadata.txt) |

## 性质逐项解释

| ID | 场景 | 要验证的内容 | 预期 | 实测 |
|---|---|---|---|---|
| H01 | Healthy | 存在从 send 到应用成功的完整路径。 | VALID | VALID |
| H02 | Healthy | ISR=3 时仅一个远端追平尚不足以推进 HW。 | VALID | VALID |
| H03 | Healthy | 提交历史满足追加、全部当前 ISR 确认与 minISR。 | VALID | VALID |
| H04 | Healthy | 应用成功必须有提交事实及 Broker 成功授权。 | VALID | VALID |
| H05 | Healthy | 时间与客户端线程持续推进时，每个操作最终形成结果，结果允许超时。 | VALID | VALID |
| H06 | Healthy | 调度负对照：不假设公平性时可永远只调度探针；此失败不是源码缺陷。 | NOT valid | NOT valid |
| S00 | WholeSafety | S01 提交条件、S02 成功来源、S03 数值范围、S04 分区锁、S05 Startup 门、S06 AFTER_APPEND 前提、S07 已成功结果稳定性：七项合取共享一次全空间搜索；通过蕴含每项通过。 | VALID | VALID |
| P01 | WholeProgress | 七角色单故障组合：明确公平前提下最终返回成功或失败。 | VALID | VALID |
| P02 | WholeProgress | 进程退出可以由 Kubelet 观察并重启。 | VALID | VALID |
| B01 | Startup | Controller TCP 已开放、Broker 处理尚未启用时，Ready 可先成立。 | VALID | VALID |
| B02 | Startup | 候选健康契约：Ready 应蕴含 Broker 处理启用；无时间模型给出反例。 | NOT valid | NOT valid |
| B03 | Startup | 冷启动路径仍遵守 Startup 门。 | VALID | VALID |
| N01 | ClientNetwork | 新 RP/LP 均至少成功两次，客户端仍可因独立链路故障交付超时。 | VALID | VALID |
| N02 | ClientNetwork | 先观察已提交、调用尚未结束且链路中断，随后客户端才交付超时。 | VALID | VALID |
| N03 | ClientNetwork | 先已提交且链路中断，随后超时，最后恢复并送达迟到响应。 | VALID | VALID |
| N04 | ClientNetwork | 错误的业务推断：超时就代表消息未提交。 | NOT valid | NOT valid |
| N05 | ClientNetwork | 链路及时恢复后仍存在成功路径。 | VALID | VALID |
| N06 | ClientNetwork | 终态稳定：迟到响应不会把已经超时的调用改成成功。 | VALID | VALID |
| E01 | Execution | 请求已接收且仍等应用结果、执行停滞，新 LP 成功两次而 Ready 保持。 | VALID | VALID |
| E02 | Execution | 候选健康契约：pgrep 成功应代表处理线程可推进；该蕴含不成立。 | NOT valid | NOT valid |
| D01 | Storage | 请求仍等应用结果且持锁等待存储，新 LP 成功两次而 Ready 保持。 | VALID | VALID |
| D02 | Storage | 候选健康契约：Ready 应蕴含存储可推进。 | NOT valid | NOT valid |
| D03 | Storage | 先观察调用仍在等待且 Broker 困于 I/O，随后 Producer 独立交付超时。 | VALID | VALID |
| R01 | Replica | 存在追加和超期未追平条件时，ISR 降至 1，持续 RP 成功。 | VALID | VALID |
| R02 | Replica | Ready 状态下客户端实际收到 ISR 不足协议错误。 | VALID | VALID |
| R03 | Replica | HW 已覆盖记录后、检查 DelayedProduce 前 ISR 缩小，可返回 AFTER_APPEND。 | VALID | VALID |
| R04 | Replica | 候选健康契约：Ready 蕴含本地分区满足最低 ISR。 | NOT valid | NOT valid |
| R05 | Replica | 错误的业务推断：Broker 返回错误就代表记录未提交。 | NOT valid | NOT valid |
| I01 | IdleReplica | 完全空闲且 LEO 已追平：只断复制链路不会因落后检查缩 ISR。 | VALID | VALID |
| I02 | IdleReplica | 负对照中 ISR=1 不可达；防止断网直接等同 ISR 不足的伪反例。 | VALID | VALID |
| Q01 | ProbeNetwork | Readiness 连续失败未到六次时缓存 Ready 可以保留。 | VALID | VALID |
| Q02 | ProbeNetwork | 先在 NotReady 状态观察仍等待的调用和成功响应，随后调用返回成功。 | VALID | VALID |
| Q03 | ProbeNetwork | 反向健康契约：NotReady 以后不应还能完成业务成功；独立路径可反驳。 | NOT valid | NOT valid |
| Q04 | ProbeNetwork | 六次已消费的连续失败后确实清除 Ready。 | VALID | VALID |
| F01 | FetchData | 健康已提交数据存在时可以完成有数据的 poll。 | VALID | VALID |
| F02 | FetchData | 不能把不存在的选中数据返回客户端。 | VALID | VALID |
| F03 | FetchEmpty | 无目标数据时 poll 正常返回空集合。 | VALID | VALID |
| F04 | FetchEmpty | 没有数据来源的场景不能产生有数据结果。 | VALID | VALID |
| F05 | FetchNetwork | Broker 存有数据但 poll 截止时仍可无数据返回。 | VALID | VALID |
| F06 | FetchNetwork | 错误的业务推断：poll 返回空集合代表 Broker 没有数据。 | NOT valid | NOT valid |
| F07 | FetchNetwork | 时间与客户端线程公平推进时 poll 有终态，允许空集合。 | VALID | VALID |
| M01 | Metadata | Leader 迁离后旧路由 Fetch 可收到协议错误并内部重试。 | VALID | VALID |
| M02 | Metadata | 元数据迁移不能在本地读取持锁期间令 I/O 悬空。 | VALID | VALID |

## 精确断言

### H01 — Healthy

存在从 send 到应用成功的完整路径。

```text
M0() reaches ProduceSuccess
```

### H02 — Healthy

ISR=3 时仅一个远端追平尚不足以推进 HW。

```text
M0() reaches TwoAckNeeded
```

### H03 — Healthy

提交历史满足追加、全部当前 ISR 确认与 minISR。

```text
M0() |= [] SafeHW
```

### H04 — Healthy

应用成功必须有提交事实及 Broker 成功授权。

```text
M0() |= [] SafeSuccess
```

### H05 — Healthy

时间与客户端线程持续推进时，每个操作最终形成结果，结果允许超时。

```text
M0() |= (([] <> time_client) && ([] <> c_poll)) -> [] (Pending -> <> Finished)
```

### H06 — Healthy

调度负对照：不假设公平性时可永远只调度探针；此失败不是源码缺陷。

```text
M0() |= [] (Pending -> <> Finished)
```

### S00 — WholeSafety

S01 提交条件、S02 成功来源、S03 数值范围、S04 分区锁、S05 Startup 门、S06 AFTER_APPEND 前提、S07 已成功结果稳定性：七项合取共享一次全空间搜索；通过蕴含每项通过。

```text
M0() |= ([] (SafeHW && SafeSuccess && Bounds && AppendLockSafe && (Ready -> Started) && AfterAppendNotAssumed)) && ([] (ProduceSuccess -> [] ProduceSuccess))
```

### P01 — WholeProgress

七角色单故障组合：明确公平前提下最终返回成功或失败。

```text
M0() |= (([] <> time_client) && ([] <> c_poll)) -> [] (Pending -> <> Finished)
```

### P02 — WholeProgress

进程退出可以由 Kubelet 观察并重启。

```text
M0() reaches Restarted
```

### B01 — Startup

Controller TCP 已开放、Broker 处理尚未启用时，Ready 可先成立。

```text
M0() reaches ReadyBeforeEnable
```

### B02 — Startup

候选健康契约：Ready 应蕴含 Broker 处理启用；无时间模型给出反例。

```text
M0() |= [] (Ready -> Enabled)
```

### B03 — Startup

冷启动路径仍遵守 Startup 门。

```text
M0() |= [] (Ready -> Started)
```

### N01 — ClientNetwork

新 RP/LP 均至少成功两次，客户端仍可因独立链路故障交付超时。

```text
Observed() reaches ReadyClientTimeout
```

### N02 — ClientNetwork

先观察已提交、调用尚未结束且链路中断，随后客户端才交付超时。

```text
Observed() reaches OrderedCommittedTimeout
```

### N03 — ClientNetwork

先已提交且链路中断，随后超时，最后恢复并送达迟到响应。

```text
Observed() reaches OrderedLateResponse
```

### N04 — ClientNetwork

错误的业务推断：超时就代表消息未提交。

```text
Observed() |= [] (ProduceTimeout -> !Committed)
```

### N05 — ClientNetwork

链路及时恢复后仍存在成功路径。

```text
Observed() reaches Recovered
```

### N06 — ClientNetwork

终态稳定：迟到响应不会把已经超时的调用改成成功。

```text
Observed() |= [] (ProduceTimeout -> [] ProduceTimeout)
```

### E01 — Execution

请求已接收且仍等应用结果、执行停滞，新 LP 成功两次而 Ready 保持。

```text
M0() reaches ExecutionPendingBlind
```

### E02 — Execution

候选健康契约：pgrep 成功应代表处理线程可推进；该蕴含不成立。

```text
M0() |= [] (lp_sample -> ExecutionCanProgress)
```

### D01 — Storage

请求仍等应用结果且持锁等待存储，新 LP 成功两次而 Ready 保持。

```text
Observed() reaches StoragePendingBlind
```

### D02 — Storage

候选健康契约：Ready 应蕴含存储可推进。

```text
Observed() |= [] (Ready -> StorageCanProgress)
```

### D03 — Storage

先观察调用仍在等待且 Broker 困于 I/O，随后 Producer 独立交付超时。

```text
Observed() reaches OrderedTimeoutInIO
```

### R01 — Replica

存在追加和超期未追平条件时，ISR 降至 1，持续 RP 成功。

```text
M0() reaches ReplicaBlind
```

### R02 — Replica

Ready 状态下客户端实际收到 ISR 不足协议错误。

```text
M0() reaches IsrRejected
```

### R03 — Replica

HW 已覆盖记录后、检查 DelayedProduce 前 ISR 缩小，可返回 AFTER_APPEND。

```text
M0() reaches AfterAppendError
```

### R04 — Replica

候选健康契约：Ready 蕴含本地分区满足最低 ISR。

```text
M0() |= [] (Ready -> MinIsrSatisfied)
```

### R05 — Replica

错误的业务推断：Broker 返回错误就代表记录未提交。

```text
M0() |= [] (BrokerError -> !Committed)
```

### I01 — IdleReplica

完全空闲且 LEO 已追平：只断复制链路不会因落后检查缩 ISR。

```text
M0() |= [] IsrIntact
```

### I02 — IdleReplica

负对照中 ISR=1 不可达；防止断网直接等同 ISR 不足的伪反例。

```text
M0() |= [] (!UnderMin)
```

### Q01 — ProbeNetwork

Readiness 连续失败未到六次时缓存 Ready 可以保留。

```text
Observed() reaches ProbeHysteresis
```

### Q02 — ProbeNetwork

先在 NotReady 状态观察仍等待的调用和成功响应，随后调用返回成功。

```text
Observed() reaches OrderedNotReadySuccess
```

### Q03 — ProbeNetwork

反向健康契约：NotReady 以后不应还能完成业务成功；独立路径可反驳。

```text
Observed() |= [] (!OrderedNotReadySuccess)
```

### Q04 — ProbeNetwork

六次已消费的连续失败后确实清除 Ready。

```text
Observed() |= [] (ReadinessThreshold -> !Ready)
```

### F01 — FetchData

健康已提交数据存在时可以完成有数据的 poll。

```text
M0() reaches PollData
```

### F02 — FetchData

不能把不存在的选中数据返回客户端。

```text
M0() |= [] (PollData -> DataAvailable)
```

### F03 — FetchEmpty

无目标数据时 poll 正常返回空集合。

```text
M0() reaches PollEmpty
```

### F04 — FetchEmpty

没有数据来源的场景不能产生有数据结果。

```text
M0() |= [] (!PollData)
```

### F05 — FetchNetwork

Broker 存有数据但 poll 截止时仍可无数据返回。

```text
M0() reaches EmptyWithData
```

### F06 — FetchNetwork

错误的业务推断：poll 返回空集合代表 Broker 没有数据。

```text
M0() |= [] (PollEmpty -> !DataAvailable)
```

### F07 — FetchNetwork

时间与客户端线程公平推进时 poll 有终态，允许空集合。

```text
M0() |= (([] <> time_client) && ([] <> c_poll)) -> [] (Pending -> <> Finished)
```

### M01 — Metadata

Leader 迁离后旧路由 Fetch 可收到协议错误并内部重试。

```text
M0() reaches RemoteError
```

### M02 — Metadata

元数据迁移不能在本地读取持锁期间令 I/O 悬空。

```text
M0() |= [] AppendLockSafe
```

## 复核与限制

- [运行清单](results/manifest.json)保存模型和输入/输出哈希；[properties.json](properties.json)保存配置与预期结果。
- 被动观察器用于 N02/N03、D03、Q02/Q03 的先后关系，仅记录历史，不改变角色状态或同步字母表。
- 首次 WholeSafety 将七项单独枚举，在 180 秒时限内只完成 S01；[原始不完整记录](results/attempts/WholeSafety-before-grouping.run.json)保留。现改成合取，共享一次搜索。未把超时当作通过。
- [变异检测记录](results/mutations/summary.json)检查性质能否发现刻意引入的单 ACK、空闲缩 ISR 和在途不超时错误；它们属于模型质量核查，不是发现了三个实现缺陷。
- 不宣称秒级漏检时长、真实全 Pod 隔离时的选举结果、无限重试、多个请求/分区或故障叠加的结论。
