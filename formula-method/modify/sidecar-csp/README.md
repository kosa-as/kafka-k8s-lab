# health-sidecar CSP 增强判定模型

本目录只有 CSP# 模型与 PAT 验证脚本，没有 Java/sidecar 运行时实现。B02 沿用原即时公式；E/D/R 使用用户确认的“异常持续并被 sidecar 确认后，下一次对应探针必须失败”的行为语义。实际 Pod Ready 的清除仍由 Kubelet 阈值处理。

## 代码如何对应设计

- `model/Observation.csp`：本地协议、本地追加和目标 ISR 的抽象采集。真实系统状态决定探测结果，状态判定过程不能直接读取故障标签。
- `model/Sidecar.csp`：连续 N 次异常或观测缺失后撤销 sidecar 就绪；连续 R 次恢复后重新就绪。执行、追加、ISR 和 UNKNOWN 分别计数。
- `model/ReadinessProbe.csp`：原生条件与 sidecar 的启动确认、就绪结果共同生成探针结果。
- `model/Kubelet.csp`：原 30/6/3 阈值与重启业务语义保留；仅在原重启事务中增加 sidecar 状态清理。
- `model/State.csp`、`System.csp`：直接引用 M0 的状态命题和业务角色；`Ready` 仍为 Kubelet 的实际就绪缓存，`Enabled`、`ExecutionCanProgress`、`StorageCanProgress`、`MinIsrSatisfied` 的含义不变。
- `model/LivenessProbe.csp`：执行路径连续 N 次失败且观测有效后，后续 Liveness 采样返回失败；存储、ISR 和观测缺失不单独触发重启。M0 没有能确认 JVM 死锁根因的观测，因此这里证明的是**本地执行路径无进展后的重启动作**，不能称为“死锁被准确定位”。

一次 `sc_begin → sc_observe → sc_decide` 与 Broker、Probe、Kubelet 交错运行。采样后的状态可以陈旧；`sc_decide` 只写 sidecar 状态，不能直接写 `ready`。所有有效采样已经采用偏乐观的结果抽象，真实网络开销和延迟只可能进一步扩大观测窗口。

默认 N=R=2，LOSS=0。LOSS=1 可表达采集缺失，但本次旧公式回归使用 LOSS=0，未扩大原故障配置。

## PAT 实测结果

| 原编号 | M0 | 候选 M1 | 说明 |
|---|---|---|---|
| B02 | NOT valid | **VALID** | 首次就绪前要求已完成请求处理启用后的探测 |
| B01 | VALID | NOT valid | 与 B02 对应的过早 Ready 坏路径不可达 |
| B03 | VALID | VALID | Startup 门保持 |
| E02 | NOT valid | **NOT valid** | 每次 `lp_sample` 都要求执行可推进；本候选保留进程型 LP |
| D02 | NOT valid | **NOT valid** | 存储故障发生后、sidecar 尚未采样前已违反即时公式 |
| R04 | NOT valid | **NOT valid** | ISR 降至最低要求以下后、Ready 尚未更新时已违反即时公式 |
| H01、H02 | VALID | VALID | 正常业务路径和当前 ISR 确认关系仍可达 |
| I01、I02 | VALID | VALID | 空闲分区不因复制链路中断直接缩 ISR |

全部 43 条旧性质都已按原场景运行。41 条有 PAT 结论；S00、P01 的组合断言在 240 秒内未完成。逐项结果和含义见[回归报告](REGRESSION.md)、[完整表](REPORT.md)。每项原始输出与依赖模型哈希在 `results/`。

### 新机制的性质与 PAT 结果

| 条件与动作 | 完整 M1 | 固定已确认状态下的实际 Probe/Kubelet |
|---|---|---|
| 执行停滞确认后，下一次 LP 采样失败 | 90/300 秒全空间搜索未完成；异常确认与 LP 失败路径可达 | **VALID** |
| 存储停滞确认后，下一次 RP 采样失败 | **VALID**；1,903,940 状态、121.43 秒 | **VALID** |
| ISR 不足确认后，下一次 RP 采样失败 | 90/300 秒全空间搜索未完成；异常确认与 Pod NotReady 路径可达 | **VALID** |
| 存储或 ISR 单独异常不重启 Kafka | 未作全组合证明 | 两个固定确认状态的性质均 **VALID** |

固定状态检查由 [DecisionContract.csp](checks/DecisionContract.csp) 使用相同的探针和 Kubelet 进程，共 8 条断言均 VALID。其结果不能替代完整 M1 的全空间结果。未完成的执行和 ISR 全组合性质**没有 PAT 结论**，不能写成 VALID。完整 M1 的三条可达性检查和存储安全性质已实际运行；原始输出在 `results/`。

针对 E02 与 R04 增加了[聚焦验证与设计说明](E02_R04_FOCUSED.md)。`ExecutionFocused.csp` 保留原故障注入、实际观测、Sidecar、双探针和 Kubelet；`ReplicaFocused.csp` 还保留原 `ClusterContext` 的背景追加、落后和 ISR 移除过程。两组各有 5 条正面断言 **VALID**、3 条预期 **NOT valid** 的对照/非空真断言。此结果加强了 E/R 的端到端局部证据，但并未完成上述完整 M1 全组合安全性质。

[PersistentResponse.csp](checks/PersistentResponse.csp) 从完整模型已证实可达的持续故障状态开始，保留相同的 Observation、Sidecar、Probe 和 Kubelet 过程。PAT 得到：存储和 ISR 的“公平采样及探针消费后最终 NotReady”均为 **VALID**；同条件原生探针均为 **NOT valid**；“公平前提不可满足”均为 **NOT valid**，说明正面性质不是空真；执行故障从持续状态进入 Kafka 重启的路径为 **VALID**。这是一项针对检测协议的有条件证明，故障如何到达该状态由完整 M1 的可达性检查支撑，不能替代完整组合的全空间活性检查。

旧性质的全量回归已运行 43 项，其中 41 项有结论：31 项原 VALID 继续 VALID，8 项原 NOT valid 继续 NOT valid，B02 由 NOT valid 转为 VALID；B01 是修复后应消失的坏路径见证，由 VALID 转为 NOT valid。S00、P01 超时。`results/ExecutionDecision.run.json` 和 `results/ReplicaDecision.run.json` 明确记录全空间安全检查在 300 秒限制内未完成，先前两项可达性检查有效，但不能将第三项写成 VALID。

## 三条反例意味着什么

**D02 是结构性不相容。** M0 的 `fe_storage_stall` 守卫要求 `life==6 && ready==1`，该原子步骤只把 `disk_ok` 改为 0。只要保留这一故障行为且允许它发生，下一状态必然有 `ready==1 && disk_ok==0`，立即反驳 `[](Ready -> StorageCanProgress)`。任何需要随后采样的 sidecar 都无法消除这个中间状态。

**R04 具有相同的异步窗口。** 最后一个远端 ISR 被移出时，`isr1+isr2==0`，原 ClusterContext 不写 Ready。调度器可先完成该步骤，再调度 sidecar 和 Kubelet，因此原即时公式仍被反驳。候选已将观测到的持续 ISR 不足纳入就绪失败，但不能追溯改变已出现的状态。

**E02 的原公式比文字描述更强。** 原式是 `[](lp_sample -> ExecutionCanProgress)`，没有要求 `lp_result` 为成功。增强 LP 在确认执行路径持续无响应后会正确返回失败，该次 `lp_sample` 仍会反驳原式。新性质检查的是“已确认执行异常时，新的 LP 采样返回失败”；没有停止故障期间的采样。

新增判定性质保留故障到采样完成之间的窗口，不修改旧命题或原业务行为。当前模型把完成的本地协议、日志追加和 ISR 探测抽象为一步，结果由 M0 的执行、存储和 ISR 状态影响；真实 sidecar 能否可靠获得这些证据，需在第五章检验。观察器本身不读取 `fault` 等故障标签。正常空闲、采集失效和正常负载下误判的全组合性质仍需补充验证。

## 复跑

```powershell
python formula-method/modify/sidecar-csp/verify.py
python formula-method/modify/sidecar-csp/verify.py --id D02 --id R04 --timeout 120
# 全部旧公式，逐项执行；可能耗时较长
python formula-method/modify/sidecar-csp/verify.py --all --timeout 120
& 'C:\Program Files\Process Analysis Toolkit\Process Analysis Toolkit 3.5.1\PAT3.Console.exe' -csp 'C:\Code\Kafka\formula-method\modify\sidecar-csp\checks\PersistentResponse.csp' 'C:\Code\Kafka\formula-method\modify\sidecar-csp\results\PersistentResponse.txt'
```

脚本从 `design/M0/checks` 和 `properties.json` 核对原公式及场景，仅将系统组合 `M0()` 替换为 `M1()`，新增 N/R/LOSS 参数。不修改 M0。PAT 进程退出码不能代替断言结果；脚本同时检查解析输出、断言数量和依赖哈希，超时标为未完成。
