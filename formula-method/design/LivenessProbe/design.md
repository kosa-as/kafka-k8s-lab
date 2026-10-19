# LivenessProbe 原生行为设计

## 研究对象与依据

本文件定义原生基准模型 M0 中**目标 Kafka 容器的 Liveness 探针**。它是由 Kubelet 调用的逻辑行为进程，不表示 Pod 内另有一个常驻探针进程。本角色返回单次检查的原始结果；失败计数与重启动作由 Kubelet 模型负责。

目标 Pod 来自 [部署脚本](../../../deploy/kafka.ps1)固定的 Bitnami Kafka Chart 32.5.0 提交 `bb5e98a6e3c5ca196420d2414769b681c9284101`，其组合角色配置见 [Helm 配置](../../../bitnami/helm-values.yaml)。固定 Chart 的 `bitnami/kafka/templates/controller-eligible/statefulset.yaml` 第 185–194 行，在未设置自定义 Liveness 探针时生成 `exec` 命令 `pgrep -f kafka`。仓库保存的 `bitnami/.helm-cache/rendered.yaml` 第 625–635 行给出以下参数：

| 参数 | 保存的渲染值 |
| --- | ---: |
| `initialDelaySeconds` | 10 秒 |
| `periodSeconds` | 10 秒 |
| `timeoutSeconds` | 5 秒 |
| `failureThreshold` | 3 次 |
| `successThreshold` | 1 次 |

保存的渲染结果还包含一个连接 Controller 端口的 Startup 探针，成功前 Liveness 不执行。按照 [Kubernetes 探针文档](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)，`exec` 探针以命令退出码 0 表示成功；连续失败达到 Liveness 阈值后，Kubelet 才触发该容器重启。

2026-10-19 后续通过本地 Kubernetes API 读取了目标 Pod；[实际探针配置快照](../Kubelet/live-probe-snapshot.json)与上述保存渲染结果一致。当前节点的 Kubelet 版本为 `v1.36.1`。

## 角色边界

**输入**：Kubelet 发出的本次探测调用；容器内执行 `pgrep -f kafka` 得到的退出结果或探测超时。模型应使用“命令是否找到匹配项”这一可观测谓词，不直接用“Broker 可服务”或“关键线程可推进”替代它。

**输出**：单次探测结果 `lp_success` 或 `lp_failure`，交给 Kubelet。

**不可见信息**：关键 JVM 线程是否死锁、请求队列是否推进、客户端端口是否服务请求、存储故障能否通过重启修复。Liveness 不能直接根据这些模型内部状态改变结果。

本角色不维护连续失败次数，也不直接重启容器。即使某次检查失败，是否达到重启阈值仍由 Kubelet 决定。

## 有限状态与事件

| 局部状态 | 事件及条件 | 下一状态与输出 |
| --- | --- | --- |
| `Idle` | `lp_invoke`，仅在 Startup 门控已通过后由 Kubelet 触发 | `RunningExec` |
| `RunningExec` | `lp_exit_zero`，`pgrep -f kafka` 在 5 秒内以 0 退出 | `Idle`，发出 `lp_success` |
| `RunningExec` | `lp_exit_nonzero`，命令以非 0 状态退出 | `Idle`，发出 `lp_failure` |
| `RunningExec` | `lp_timeout`，命令未在 5 秒内完成 | `Idle`，发出 `lp_failure` |
| 任意状态 | `container_restart` | 重置为 `Idle`；后续调度重新受 Startup 门控约束 |

`lp_invoke`、探测期限、连续失败计数由 Kubelet 控制。模型不应把 `lp_success` 定义为 JVM 无死锁；它只表示这次命令按原生规则成功。CSP# 实现时需明确事件同步和超时推进，使等待检查结果不会错误地阻塞其他系统行为。

## 与其他角色的接口及候选反例

本目录的 [LivenessProbe.csp](LivenessProbe.csp) 实现上述单次探针状态机，可由 Kubelet 直接包含；[LivenessProbeVerify.csp](LivenessProbeVerify.csp)单独保存分支覆盖标志和断言。`container_restart` 是外部中断事件，并非 Liveness 一次失败就直接重启。[PAT 验证记录](verification.txt)显示 6 条断言有效。现有 [Kubelet 组合模型](../Kubelet/Kubelet.csp)已接入探针进程，并实现 Startup 门控和结果计数；进程命令的实际运行结果仍需后续容器模型约束。这些断言尚未验证 Kafka 关键 JVM 执行能否推进。

`KafkaBroker.Lifecycle` 和容器进程表决定是否存在 `pgrep` 可匹配的进程；`KafkaBroker.Execution` 的关键执行进展是另一个独立状态。`Kubelet` 发起探测、接收 `lp_success/lp_failure` 并按阈值决定是否重启。`FaultEnvironment` 可以导致执行路径阻塞，但不能直接改写 Liveness 的观测结果。

待组合模型完成后，应检查一种**候选轨迹**：Kafka 命令行仍能被 `pgrep` 匹配，因此 Liveness 连续成功，但某条关键 JVM 执行路径持续不能推进。只有当该停滞被论文目标明确界定为“重启有望恢复的本地故障”，它才构成待解释的 Liveness 漏检反例。外部存储或网络故障即使造成请求失败，也不能据此要求 Liveness 必须重启。

## 待核对事项

1. 目标 Pod 的实际 `livenessProbe` 与 `startupProbe` 已核对；重新部署或修改配置后需要重新读取。
2. 核对固定镜像中 `pgrep` 的执行环境及 Kafka 进程命令行，确认“匹配项存在”与实际容器进程状态的关系。
3. 在 Kubelet 模型中实现 Startup 门控、连续 3 次失败后的重启，以及重启后局部状态重置；不得假定重启会消除外部故障。
