# health-sidecar CSP 增强判定模型

论文范围与术语按 [最新第一、二章研究基线](../../../docs/THESIS_CONSTRAINTS.md) 对齐。本目录承接 RQ2 的判定机制与 RQ3 的形式化比较；实机检测效果和业务表现由第五章验证。健康事件是计数状态的解释标签；平台动作属于输出传播的环境行为，不构成新的恢复策略贡献。

本目录只有 CSP# 模型与 PAT 验证脚本，没有 Java/sidecar 运行时实现。B02 沿用原即时公式；E/D/R 使用用户确认的“异常持续并被 sidecar 确认后，下一次对应探针必须失败”的行为语义。实际 Pod Ready 的清除仍由 Kubelet 阈值处理。

## 当前机制：按项确认异常，完整健康恢复

2026-11-18 版本使用三个 `sc_*_valid` 和三个 `sc_*_unknown`。`sc_valid` 仅汇总观测完整性；某项未知不屏蔽其他项有效证据。执行有效失败独立累计，存储失败需要执行有效成功，ISR有效不足独立累计。未确认计数被本项缺失中断，已确认计数保持到取得有效恢复证据。三项完整健康连续R轮后恢复就绪。详细规则见 [PER_METRIC.md](PER_METRIC.md)。

- `Observation.csp`：原子提交每项结果与有效位；LOSS=0为完整采集，LOSS=1额外允许7类非空缺失子集。
- `Sidecar.csp`：维护三个故障计数、三个缺失计数和恢复计数；饱和故障计数同时保存已确认事件。
- `ReadinessProbe.csp`：原生条件与sc_started/sc_ready的合取；`LivenessProbe.csp`：进程存在且没有保留的执行确认。LP不再依赖最新一轮sc_valid。
- `Kubelet.csp`：沿用原30/6/3阈值；重启清除本代所有观测、计数与确认。
- M0业务角色与原性质不变；采样、决策、探针、平台更新仍交错，Sidecar不直接写平台ready。

## 当前源码的PAT复跑结果

| 检查组 | 结果与范围 |
|---|---|
| 43条原性质，LOSS=0 | 41完成：31原VALID保持、8原NOT valid保持；B01由VALID变NOT valid、B02相反。S00/P01各240秒未完成 |
| PerMetric系列，15组40条 | 全部VALID。指定输入序列及实际Observation的局部组合；验证部分缺失下独立确认、确认保持、解除、恢复、交替缺失、LP边界和重启清理 |
| DecisionContract | 8条VALID，固定已确认状态下的实际Probe/Kubelet |
| ExecutionFocused / ReplicaFocused | 各5条正面VALID、3条负对照NOT valid，保留相应故障来源与检测链路 |
| PersistentResponse | 存储/ISR公平条件响应及执行重启可达共3条VALID，4条原生或非空性对照NOT valid |
| 完整M1 StorageDecision | 3条VALID；安全性质1,903,940状态，约174.75秒 |
| 完整M1 ExecutionDecision / ReplicaDecision | 各2条可达性VALID；第三条安全检查各300秒未完成 |

分项缺失的40条结果不等同于全部业务角色与LOSS=1任意缺失交错的全空间证明。原即时E02、D02、R04仍为NOT valid：执行旧公式约束每次采样，存储/ISR旧公式仍存在故障到平台更新的异步窗口。重启可达不表示故障必然修复。

所有现用结果都附当前模型与检查入口哈希。旧版源码/输出已归档于 `.planning/2026-11-18-m1-per-metric-observation-revision/baseline-m1/`，旧版拆分检查仅作历史记录。当前完整回归见 [REPORT.md](REPORT.md) 和 [REGRESSION.md](REGRESSION.md)。

## 复跑

```powershell
python formula-method/modify/sidecar-csp/verify.py --all --timeout 240
python formula-method/modify/sidecar-csp/verify_additional.py --timeout 60
python formula-method/modify/sidecar-csp/verify_additional.py --id DecisionContract --id ExecutionFocused --id ReplicaFocused --id PersistentResponse --id StorageDecision --id ExecutionDecision --id ReplicaDecision --timeout 300
```

PAT版本为3.5.1。超时保留已完成断言，未完成断言没有结论；完整检查输出、输入及模型哈希在 `results/`。这里仍是CSP#模型，实际采集映射由第五章核验。
