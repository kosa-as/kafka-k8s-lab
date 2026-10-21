# M0-R2：七角色整体模型

本目录是当前建模入口。目标是在固定版本源码与部署配置支持的边界内，检查探针状态、Broker 请求处理、分区复制条件与客户端结果之间的关系。**PAT 的 NOT valid 首先是某条模型性质被反驳；只有对照源码、抽象约束和实机证据后，才能判断是否属于实现缺陷。**

| 文件/目录 | 职责 |
|---|---|
| [DESIGN.md](DESIGN.md) | 七角色结构、变量所有权、原子边界、状态压缩与假设 |
| [SOURCE_AUDIT.md](SOURCE_AUDIT.md) | 固定版本源码依据、旧模型问题及修正 |
| [model/State.csp](model/State.csp) | 公共状态和可观测命题 |
| `model/` 下七个角色文件 | 各角色自身的受保护原子转移，无断言 |
| [model/System.csp](model/System.csp) | 唯一七角色组合 `M0()` |
| `checks/*.csp` | 配置常量和带中文说明的独立验证入口 |
| [VERIFICATION.md](VERIFICATION.md) | 每条性质、场景、预期、实测、状态数和日志链接 |
| [FINDINGS.md](FINDINGS.md) | 缺陷候选、语义误用、模型修正与反例解释 |
| [results/manifest.json](results/manifest.json) | PAT 二进制、模型、入口及结果的 SHA-256 和运行信息 |
| [evidence/sources.json](evidence/sources.json) | 已下载原始源码的版本 URL 和内容哈希 |

## 运行

在仓库根目录执行：

```powershell
python formula-method/design/M0/build_checks.py
python formula-method/design/M0/verify.py --timeout 240
python formula-method/design/M0/report.py
```

模型质量核查可运行 `python formula-method/design/M0/check_mutations.py`，在临时副本中注入三种已知抽象错误，检查对应性质是否能报告反例。

也可在 PAT 3.5.1 GUI 中直接打开 `checks/Healthy.csp` 或其他入口。所有 `#include` 使用相对路径；角色文件自身不含 `#assert`，不会隐式重复运行其他组合的断言。

`verify.py --suite Replica` 可单独复跑一个入口；用 `--pat` 指定其他安装路径。脚本检查断言结果数量和预期结果，不能只凭 PAT 的退出码判断成功。NOT valid 如果是预先登记的候选契约或负对照，视为预期的检测结果；它不会被改写为 VALID。

本版未重新部署集群、未注入实机故障。当前结果属于前期模型验证。旧实机快照只作为配置与起始分区条件依据，不作为新版反例已经实测发生的证据。
