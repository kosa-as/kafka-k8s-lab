# Kubelet 探针控制 M0 设计

## 对象、版本与证据

本角色表示目标 `bitnami-kafka-controller-0` Pod 中 **Kafka 容器**的 Kubelet 探针控制逻辑，初始状态对应一个新创建、尚未通过 Startup 探针的容器。`kubectl` 仅用于读取集群状态，不是此 CSP# 角色。2026-10-19 读取的 [本地配置快照](live-probe-snapshot.json)确认：节点 Kubelet 为 `v1.36.1`、Pod 的 `restartPolicy` 为 `Always`，Kafka 容器同时配置了 Startup、Readiness 和 Liveness 探针。Startup 与 Readiness 均连接 Controller 端口 9093；Liveness 执行 `pgrep -f kafka`。对应失败阈值依次为 **30、6、3**，成功阈值均为 1。

源码依据固定在 Kubernetes `v1.36.1`：

| 行为 | 源码依据 | 模型对应 |
| --- | --- | --- |
| 三类探针的初始缓存值分别为 Unknown、Failure、Success | [`worker.go` 112–124 行](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/kubelet/prober/worker.go#L112-L124) | 新容器 `started=0`、`kafka_ready=0`，Liveness 在达到阈值前不触发重启 |
| Startup 成功前停用 Readiness/Liveness | [`worker.go` 330–345 行](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/kubelet/prober/worker.go#L330-L345)、[`prober_manager.go` 275–289 行](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/kubelet/prober/prober_manager.go#L275-L289) | 仅 `KRunning` 可发出 `rp_invoke` 和 `lp_invoke` |
| 连续相同结果达到阈值后才更新结果缓存；Startup/Liveness 达到终止结果后等待新容器 | [`worker.go` 366–390 行](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/kubelet/prober/worker.go#L366-L390) | `sp_fail_run`、`rp_fail_run`、`lp_fail_run`，以及阈值事件 |
| Readiness 缓存 Success 决定该容器 Ready | [`prober_manager.go` 332–350 行](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/kubelet/prober/prober_manager.go#L332-L350) | `kafka_ready`，成功一次设为 1，连续失败六次后设为 0 |
| TCP 连接失败及超时返回 Failure；exec 非零退出和命令超时返回 Failure | [`tcp.go`](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/probe/tcp/tcp.go)、[`exec.go`](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/probe/exec/exec.go) | `sp_*`、`rp_*`、`lp_*` 原始结果路径 |
| Liveness/Startup 缓存 Failure 使运行中的容器进入 kill/restart 路径 | [`kuberuntime_manager.go` 1327–1373 行](https://github.com/kubernetes/kubernetes/blob/v1.36.1/pkg/kubelet/kuberuntime/kuberuntime_manager.go#L1327-L1373) | `startup_threshold` 或 `liveness_threshold` 后触发 `container_restart` |

配置数值来自实际 Pod Spec；Kubelet 源码决定这些数值怎样作用于探针结果。`kafka_ready` 只表示 Kafka 容器的 Ready 贡献，不能直接当作整个 Pod Ready，因为当前 Pod 还有其他容器。

## 三个角色如何串联

[Kubelet.csp](Kubelet.csp)使用相对路径直接包含 [ReadinessProbe.csp](../ReadinessProbe/ReadinessProbe.csp) 与 [LivenessProbe.csp](../LivenessProbe/LivenessProbe.csp)，组成 `System() = KStartup() || RPIdle() || LPIdle()`。Kubelet 发出 `rp_invoke`／`lp_invoke`，对应探针报告 `rp_success`／`rp_failure`、`lp_success`／`lp_failure`，Kubelet 随后提交计数和缓存更新。`container_restart` 同步重置三个角色。Startup 探针是 Kubelet 内部子行为，不增加第八个顶层角色。各探针的独立断言位于同目录的 `*Verify.csp`，不会增加组合模型的状态。

PAT 的自动字母表不会计入带赋值的事件；因此跨角色握手使用**不带赋值的纯事件**，计数更新放在随后的 `k_commit_*` 事件，且显式声明 Kubelet 的共享事件字母表。这保证 `container_restart`、探针调用和原始结果确实在角色之间同步。

| 阶段 | 关键转换 |
| --- | --- |
| Startup | `sp_invoke` 后 TCP 成功使 `started=1`；失败或超时增加连续失败数；第 30 次连续失败触发 `startup_threshold` |
| Readiness | Startup 成功后才调用；一次成功清零失败数并设置 `kafka_ready=1`；第 6 次连续失败设置 `kafka_ready=0`；单次失败不触发重启 |
| Liveness | Startup 成功后才调用；成功清零失败数；第 3 次连续失败触发 `liveness_threshold` |
| 重启 | Startup 或 Liveness 阈值事件后，`container_restart` 重置门控、Kafka Ready 及三个计数，再重新进入 Startup |

## 状态压缩与验证范围

实际 Kubelet 为每类探针建立独立 worker，可能交错执行。为了控制首个跨角色模型的状态空间，本版在 `KRunning` 中**一次仅调度一个探针执行**，但保留每次原始结果和 **30／6／3 的精确连续计数**。计时器、初始延迟、运行时重启退避、Pod 状态向 API 的传播延迟和其他容器的状态不进入此无时间模型；因此不能从本版推导真实秒数或探针重叠时的竞态结论。

意外的探针执行错误在 `worker.go` 中可能被丢弃而不计数；本版只覆盖 TCP 成功／失败／超时及 exec 零退出／非零退出／超时的常规路径。后续如果研究这类错误，应为探针角色和 Kubelet 同时增加独立的 `error` 事件，不能把它直接当作 Failure。

[PAT 验证记录](verification.txt)来自本机 PAT 3.5.1：组合模型的 **18 条断言全部 VALID**，覆盖无死锁、Started／Ready 和两种阈值的可达性，以及 Startup 门控、阈值一致性、跨角色结果提交、重启重置等 LTL 性质。两份探针的独立模型各 6 条断言也重新通过。这里证明的是所述抽象合同；业务请求能否完成及原生探针是否漏检，仍需与 KafkaBroker、FaultEnvironment 等角色组合后检查。
