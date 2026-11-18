# M0 性质在 sidecar M1 中的回归结果

当前分项有效性版本复跑日期：2026-11-18。验证工具：PAT 3.5.1。脚本 `verify.py` 从 M0 的 `properties.json` 与原检查文件读取 43 条断言，保持场景、故障参数和命题含义，将组合入口换成当前 M1。原始输出、输入及模型哈希保存在 `results/<ID>.txt` 和 `results/<ID>.json`；全表见 [REPORT.md](REPORT.md)。

**41/43 条已完成验证。** 31 条原 VALID 继续 VALID，8 条原 NOT valid 继续 NOT valid。两项有意改变：B02 从 NOT valid 变为 VALID；B01 从 VALID 变为 NOT valid，因为 B01 原本是“处理未启用就 Ready”的坏路径可达性见证。S00、P01 的原组合断言分别运行 240 秒仍未完成，当前没有 PAT 结论。超时不能记为 VALID，也没有发现反例。

| 类别 | 结果 | 代表性质 |
|---|---|---|
| 业务安全、提交和终态 | 已完成项保持原结果 | H01—H05、N06、F01—F07、M02 |
| 启动门控 | 原缺陷被消除 | B01 不可达；B02 VALID；B03 VALID |
| 故障路径与探针缓存 | 已完成项保持原结果 | E01、D01/D03、R01—R03、Q01—Q04 |
| 负对照与错误业务推断 | 保留原反例 | H06、N04、E02、D02、R04/R05、Q03、F06 |
| 组合性质 | 未得结论 | S00、P01 |

上表把 `reaches` 的结果按命题内容解释：一些原 VALID 证明正确路径存在，另一些证明缺陷路径存在。B01 的结果改变符合修复目标，不能被算作破坏了正确行为。E02、D02、R04 的**即时旧公式**仍被反驳；其对应的增强判定效果由 [DecisionContract.csp](checks/DecisionContract.csp)、[PersistentResponse.csp](checks/PersistentResponse.csp) 和三类完整模型检查另行验证，不能将新性质的 VALID 冒称旧公式通过。

S00/P01 使用 `FAULT=-1` 覆盖全部单故障。为保留原公式同时定位耗时来源，`decompose.py` 将 `FAULT` 分别固定为健康和七类单故障。历史版本30秒初筛中 `S00_F0`、`P01_F0` 为VALID，其余未完成；这些拆分结果未针对当前模型复跑，不作为当前版本结论。输入、输出和耗时位于 `checks/decomposed`、`results/decomposed`。

当前 M1 的可观测性仍属抽象：`Observation` 根据被测系统的执行、存储和 ISR 条件产生完成的探测结果；`Sidecar` 只读取探测结果。CSP 中得到的结论取决于探测能够反映相应服务能力，第五章必须验证真实 sidecar 是否具备这些观测能力。

分项缺失与确认保持机制另有15组40条当前检查，均VALID，见 [PER_METRIC.md](PER_METRIC.md) 与 `results/PerMetric*.run.json`。此组证据限定于所检查的输入序列和局部组合。
