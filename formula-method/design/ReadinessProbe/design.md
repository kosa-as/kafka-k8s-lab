# ReadinessProbe 原生行为设计

## 研究对象与依据

本文件定义原生基准模型 M0 中**目标 Kafka 容器的 Readiness 探针**。它是 CSP# 中的逻辑行为进程，不表示 Pod 内另有一个探针进程。探针由 Kubelet 调用；本角色只描述一次探测所能读取的信号和返回的原始结果。失败计数、容器 Ready 状态及 Pod Ready 状态由后续的 Kubelet 模型维护。

当前仓库通过 [部署脚本](../../../deploy/kafka.ps1)固定 Bitnami Kafka Chart 32.5.0 的提交 `bb5e98a6e3c5ca196420d2414769b681c9284101`。[Helm 配置](../../../bitnami/helm-values.yaml)设置 `controller.replicaCount: 3`、`controller.controllerOnly: false`、`broker.replicaCount: 0`，因此目标是一个同时具有 Controller 和 Broker 角色的 Pod。固定 Chart 的 `bitnami/kafka/templates/controller-eligible/statefulset.yaml` 第 195–201 行，在未设置自定义 Readiness 探针时生成 `tcpSocket` 到名为 `controller` 的端口。仓库保存的 `bitnami/.helm-cache/rendered.yaml` 第 636–643 行显示该端口为 9093，且 Kafka 容器的参数如下：

| 参数 | 保存的渲染值 |
| --- | ---: |
| `initialDelaySeconds` | 5 秒 |
| `periodSeconds` | 10 秒 |
| `timeoutSeconds` | 5 秒 |
| `failureThreshold` | 6 次 |
| `successThreshold` | 1 次 |

Chart 模板同时生成一个连接 Controller 端口的 Startup 探针；保存的渲染值为初始延迟 10 秒、周期 10 秒、超时 5 秒、失败阈值 30 次。Kubernetes 在 Startup 探针成功前不会执行 Readiness 探针；启动门控属于 Kubelet 角色。[Kubernetes 探针文档](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)也说明，TCP 探针以能否建立连接判断成功，而不是执行 Kafka 协议请求。

2026-10-19 后续通过本地 Kubernetes API 读取了目标 Pod；[实际探针配置快照](../Kubelet/live-probe-snapshot.json)与上述保存渲染结果一致。当前节点的 Kubelet 版本为 `v1.36.1`。

## 角色边界

**输入**：Kubelet 发出的本次探测调用；目标 Pod 的 Controller 端口能否在探测期限内建立 TCP 连接。连接结果由 Broker 的 Controller 监听状态与探测路径环境共同决定，探针不能直接读取这两个内部原因。

**输出**：单次探测结果 `rp_success` 或 `rp_failure`，交给 Kubelet。

**不可见信息**：客户端端口是否可用、请求处理线程是否推进、Produce/Fetch 是否完成、磁盘 I/O 是否停滞、ISR 是否满足条件。这些状态可以用于系统性质评价，但不得作为 M0 Readiness 的判断输入。

本角色不设置 `Ready=true/false`，也不执行重启。Readiness 失败达到阈值后的状态变化由 Kubelet 模型表达。若检查的是整个 Pod 的 Ready 条件，还需明确其他容器就绪状态；初版可显式假定其他容器保持就绪，只分析 Kafka 容器的贡献。

## 有限状态与事件

| 局部状态 | 事件及条件 | 下一状态与输出 |
| --- | --- | --- |
| `Idle` | `rp_invoke`，仅在 Startup 门控已通过后由 Kubelet 触发 | `Connecting` |
| `Connecting` | `rp_tcp_connected`，在 5 秒期限内建立到 Controller 端口的连接 | `Idle`，发出 `rp_success` |
| `Connecting` | `rp_tcp_failed`，连接被拒绝或探测路径不可达 | `Idle`，发出 `rp_failure` |
| `Connecting` | `rp_timeout`，到达 5 秒期限仍未建立连接 | `Idle`，发出 `rp_failure` |
| 任意状态 | `container_restart` | 重置为 `Idle`；后续调度重新受 Startup 门控约束 |

`rp_invoke`、探测期限和连续结果计数由 Kubelet 控制。CSP# 实现时，以上事件名只是接口草案；共享事件及进程字母表应显式核对，避免错误同步。正常探测周期是配置参数，模型不能假设 Kubernetes 始终以精确 10 秒间隔运行探针。

## 与其他角色的接口及候选反例

本目录的 [ReadinessProbe.csp](ReadinessProbe.csp) 实现上述单次探针状态机，可由 Kubelet 直接包含；[ReadinessProbeVerify.csp](ReadinessProbeVerify.csp)单独保存分支覆盖标志和断言。`container_restart` 是外部中断事件，允许正在进行的探针被放弃；它不表示 Readiness 触发重启。[PAT 验证记录](verification.txt)显示 6 条断言有效。现有 [Kubelet 组合模型](../Kubelet/Kubelet.csp)已接入探针进程，并实现 Startup 门控和结果计数；TCP 结果仍由后续 Broker 与故障环境模型约束。这些断言尚未验证 Kafka 业务请求是否可完成。

`KafkaBroker.NetworkHandler` 提供 Controller 监听端口的状态；`FaultEnvironment` 提供 Kubelet 到该端口的探测路径条件；`Kubelet` 发起探测并接收 `rp_success/rp_failure`。`ClusterContext` 中的 Controller 法定人数或元数据状态不能自动等同于“Controller TCP 端口可连接”。

待组合模型完成后，应检查一种**候选轨迹**：Controller 端口持续可连接，Readiness 因而持续成功，同时目标 Kafka 服务职责因另一条执行路径受阻。若该轨迹可达且违反论文预先定义的健康目标，结论是原生 Readiness 的观测范围不足；它本身不证明 Chart 或 Kubernetes 违反了各自既定语义。故障可达性、持续时间和 Probe 获得执行机会都必须写进性质前提。

## 待核对事项

1. 目标 Pod 的实际 `readinessProbe`、`startupProbe` 和端口映射已核对；重新部署或修改配置后需要重新读取。
2. 核对 Kafka 进程的 Controller 监听状态与客户端服务路径之间可以出现哪些独立组合；只把源码和可复现行为支持的组合放入 M0。
3. 在 Kubelet 模型中实现 Startup 门控、连续失败阈值以及 Kafka 容器 Ready 到 Pod Ready 的关系。
