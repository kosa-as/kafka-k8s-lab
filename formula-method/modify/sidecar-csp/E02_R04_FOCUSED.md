# E02、R04 的机制收敛与聚焦验证

## 设计目标与旧性质的边界

E02 原式 `[](lp_sample -> ExecutionCanProgress)` 在执行停滞后，即便 LP 正确返回失败，也会因发生了采样而被反驳。R04 原式 `[](Ready -> MinIsrSatisfied)` 在最后一个 ISR 成员移出与 Kubelet 更新 Ready 之间必有异步窗口。因此，本设计不声称旧即时公式转为 VALID，而检验 sidecar 的**确认、探针动作、Kubelet 处置**是否形成闭环。这里没有修改 M0 的故障注入、ISR 变化或 Kubelet 阈值。

## E02：执行停滞

1. M0 的 `fe_execution_stall` 将 `exec_ok` 置 0。`Observation` 完成一次本地执行路径探测，向 sidecar 提供 `sc_exec=0`；决策只读探测结果，不读故障标签。
2. `Sidecar` 连续 `N=2` 次有效失败后令 `sc_exec_fail=N`、`sc_ready=0`。
3. `LivenessProbe` 在确认后下一次采样返回失败；原 Kubelet 连续 3 次 LP 失败后达到重启阈值。重启动作仍为原来的独立 `k_restart`。

[`checks/ExecutionFocused.csp`](checks/ExecutionFocused.csp) 保留真实故障注入、观测、sidecar、双探针与 Kubelet，省略与执行路径判定无关的 Client 和 Cluster 进程。PAT 结果（[`results/ExecutionFocused.txt`](results/ExecutionFocused.txt)）：

| 检查 | 结果 |
|---|---|
| 连续失败确认可达 | VALID |
| 故障后 LP 失败或重启动作可达 | VALID |
| 已确认时，下一次 LP 采样必失败 | VALID |
| 实际重启状态可达 | VALID |
| 故障发生、阈值前公平调度观测与 LP 时，最终达到 LP 连续 3 次失败 | VALID |
| “上述公平前提不可满足” | NOT valid，即存在满足前提的路径 |
| 同一场景改用 M0 的进程存活探针，最终达到 LP 失败阈值 | NOT valid |
| “原生探针对照的公平前提不可满足” | NOT valid，即公平前提有可行路径 |

公平前提只约束尚未达到阈值且尚未重启的阶段；重启后 M0 的一次性生命周期停止采样。**不能据此断言每条公平路径最终执行 `k_restart`**：PAT 可在阈值已达到后无限调度其他事件，让已使能的重启动作饥饿。该调度问题属于模型的动作公平性边界，不代表 sidecar 未识别停滞。完整 M1 中 E02 新安全性质此前在 300 秒内未完成，仍无全空间结论。

## R04：目标 ISR 不足

1. M0 的 `fe_replica_cut`、背景追加、落后期限与 `cc_remove_replica1/2` 使目标分区在本地仍为 Leader 时落到 `isr1+isr2=0`。只针对本地 Leader 检查最低 ISR；Follower 不因本分区的 ISR 不足被判故障。
2. `Observation` 从目标 ISR 得到 `sc_isr=0`；连续 `N=2` 次有效失败后，sidecar 撤销自身就绪。
3. 后续 RP 采样失败，原 Kubelet 达到连续 6 次失败后清除 `ready`。ISR 不足不会单独触发 LP 失败或 Kafka 重启。

[`checks/ReplicaFocused.csp`](checks/ReplicaFocused.csp) 还保留了 M0 的 `ClusterContext`，因此验证路径含原生背景追加、滞后与 ISR 移除，而不是从人工构造的 `isr=0` 状态开始。PAT 结果（[`results/ReplicaFocused.txt`](results/ReplicaFocused.txt)）：

| 检查 | 结果 |
|---|---|
| 本地 Leader、ISR 不足且已确认的状态可达 | VALID |
| ISR 不足且 Pod NotReady 的状态可达 | VALID |
| 已确认时，下一次 RP 采样必失败 | VALID |
| ISR 故障本身不引发 Kafka 重启 | VALID |
| 持续 ISR 不足且公平调度观测与 RP 时，最终 NotReady | VALID |
| “上述公平前提不可满足” | NOT valid，即存在满足前提的路径 |
| 同一场景改用 M0 的 TCP 就绪探针，最终 NotReady | NOT valid |
| “原生探针对照的公平前提不可满足” | NOT valid，即公平前提有可行路径 |

正常 Kubelet 就绪阈值带来的延迟仍存在；旧 R04 的即时公式依旧 NOT valid。完整 M1 的 R04 新安全性质此前在 300 秒内未完成，不能把聚焦结果写为完整模型的全空间结论。

## 范围和复跑

本次优化是将 E02/R04 的机制合同落实到**带实际故障来源的聚焦组合**，补充非空真检查，并在相同环境下替换为 M0 原生探针作对照；核心探测与 Kubelet 代码没有变动。对照进程按 M0 的探针过程逐句复制，仅更名以便在同一个检查文件中选择。聚焦组合覆盖上述变量的写入者，不覆盖 Broker、Client 与所有故障类型同时交错时的状态空间。两组结果证明机制在相应单故障场景下可达、按确认结果正确动作，并在明确公平前提下推进到阈值或 NotReady；相同响应义务在原生探针下不成立。

```powershell
$pat = 'C:\Program Files\Process Analysis Toolkit\Process Analysis Toolkit 3.5.1\PAT3.Console.exe'
& $pat -csp 'C:\Code\Kafka\formula-method\modify\sidecar-csp\checks\ExecutionFocused.csp' 'C:\Code\Kafka\formula-method\modify\sidecar-csp\results\ExecutionFocused.txt'
& $pat -csp 'C:\Code\Kafka\formula-method\modify\sidecar-csp\checks\ReplicaFocused.csp' 'C:\Code\Kafka\formula-method\modify\sidecar-csp\results\ReplicaFocused.txt'
```

M1 原模型文件未变；`verify.py --resume` 对 H01—H04、B03、I01/I02、N06、Q04、M02 的模型哈希核对后均复用 VALID 结果。
