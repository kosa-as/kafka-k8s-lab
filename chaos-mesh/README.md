# Chaos Mesh：Kafka 故障注入使用说明

已于 2026-10-05 在 `docker-desktop` 安装 Chaos Mesh **2.8.4**，命名空间为 `chaos-mesh`。控制器、Daemon、Dashboard、DNS 均已就绪，Dashboard 数据使用 `hostpath` 的 1Gi PVC 保存。

`deploy/all.ps1` 不会自动安装 Chaos Mesh 或执行故障实验。仓库中的三个示例默认只是清单文件，`kubectl apply` 会立即创建真实实验。集群中可能保留以前创建的实验对象；每次操作前先用 `kubectl --context docker-desktop -n kafka get podchaos,networkchaos` 检查现状，避免重复执行。

## 1. 打开 Dashboard 并登录

访问 <http://127.0.0.1:32333>。本机已验证这个地址返回 HTTP 200；若 `localhost` 被代理或 IPv6 解析影响，请使用 `127.0.0.1`。

在 PowerShell 中生成登录 Token：

```powershell
kubectl --context docker-desktop -n kafka create token chaos-kafka-manager --duration=8h
```

在 Dashboard 的 Token 登录框中填写：Name 为 `kafka-manager`，Token 为上面命令的输出。该账号可以管理 `kafka` 命名空间中的 Chaos Mesh 资源，不能管理其他命名空间的实验，也不是 cluster-admin。Token 过期后重新生成；不需要保存到 Git。

如果 NodePort 无法访问，在独立 PowerShell 窗口运行并保持窗口打开：

```powershell
kubectl --context docker-desktop -n chaos-mesh port-forward --address 127.0.0.1 svc/chaos-dashboard 2333:2333
```

然后访问 <http://127.0.0.1:2333>。关闭转发窗口只关闭网页访问，不会停止运行中的实验。

## 2. 可以用什么故障测试 Kafka

| 示例文件 | 实际作用 | 如何恢复 |
| --- | --- | --- |
| `examples/kafka-container-kill.yaml` | 只杀死 `bitnami-kafka-controller-0` 中的 `kafka` 容器一次，保留其他 sidecar | kubelet 根据 Pod 的重启策略重启 Kafka；恢复耗时取决于 Kafka 启动和副本追赶 |
| `examples/kafka-network-delay.yaml` | 节点 0 发往节点 1、2 的流量增加 200ms 延迟、20ms 抖动 | 注入成功后持续约 60s，控制器正常工作时自动撤销网络规则 |
| `examples/kafka-network-partition.yaml` | 阻断节点 0 与节点 1、2 的双向流量 | 注入成功后持续约 60s，控制器正常工作时自动撤销网络规则 |

网络故障作用于 Pod 网络命名空间，因此会影响同 Pod 中使用这些连接的 sidecar。隔离示例只隔离三个 Kafka Pod 之间的通信，不阻断节点 0 与所有客户端的连接。

这三个 Pod 同时承担 broker/controller 角色，所以节点 0 的故障可能同时触发分区 leader 切换和 KRaft 选举。网络规则撤销不代表 Kafka 已完成副本追赶。

如果只是了解流程，可以打开 YAML、查看 Dashboard 的新建实验表单，但不要点击 Submit/Create。**点击提交或者不带 dry-run 执行 kubectl apply，就会立即注入故障。**

Dashboard 手动配置时选 `kafka` 命名空间，使用标签 `statefulset.kubernetes.io/pod-name=bitnami-kafka-controller-0`，确认预览只匹配这个 Pod。Container Kill 的容器名填写 `kafka`；网络实验设置 60s 时长。要精确复用本说明的对端范围，优先使用已给出的 YAML。

## 3. 仅校验 YAML，不执行

在项目根目录 `C:\Code\Kafka` 执行：

```powershell
kubectl --context docker-desktop apply --dry-run=server -f chaos-mesh/examples/kafka-container-kill.yaml
kubectl --context docker-desktop apply --dry-run=server -f chaos-mesh/examples/kafka-network-delay.yaml
kubectl --context docker-desktop apply --dry-run=server -f chaos-mesh/examples/kafka-network-partition.yaml
```

输出含 `server dry run` 表示 API 校验通过，资源不会保存，控制器不会执行故障。这只能验证 schema/webhook，不能证明真实故障注入成功。

## 4. 实际执行的步骤

先确认 context 和三个 Kafka Pod 正常，再准备持续的生产/消费流量，记录吞吐、延迟、超时、消费 lag 和 ISR 基线。已有 Prometheus 可以辅助观察；仅查看 Pod 的 Running 状态不足以判断 Kafka 服务是否恢复。

```powershell
kubectl config current-context
kubectl --context docker-desktop -n kafka get pods bitnami-kafka-controller-0 bitnami-kafka-controller-1 bitnami-kafka-controller-2
```

一次只选一个实验。例如，以下命令会立即执行网络延迟实验：

```powershell
# 真实故障注入：准备好测试时才执行这一条。
kubectl --context docker-desktop apply -f chaos-mesh/examples/kafka-network-delay.yaml
```

不要直接 apply 整个 `examples` 目录，否则会同时运行三个实验。要换成容器重启或网络隔离，单独选择对应文件，待上一个实验恢复并清理后再运行。

观察实验状态及 Kafka 状态：

```powershell
kubectl --context docker-desktop -n kafka describe networkchaos kafka-0-network-delay
kubectl --context docker-desktop -n kafka get networkchaos kafka-0-network-delay -o yaml
kubectl --context docker-desktop -n kafka get pods -w
kubectl --context docker-desktop -n kafka logs bitnami-kafka-controller-0 -c kafka --tail=100
```

查看实验的 Conditions、Records、Events，确认注入、恢复是否成功；同时比较业务指标和 Kafka 日志。Container Kill 时可查看 `podchaos kafka-0-container-kill`，以及 Kafka 容器的重启次数和 `logs --previous`。

`health-sidecar` 只定期写状态文件，不能反映 Kafka 的真实网络和副本健康；评估效果应以实际生产消费请求、ISR 和 Kafka 指标为准。

## 5. 提前停止、清理与再次运行

网络延迟还没结束时，可先暂停，控制器会尝试撤销注入的规则：

```powershell
kubectl --context docker-desktop -n kafka annotate networkchaos kafka-0-network-delay experiment.chaos-mesh.org/pause=true --overwrite
```

确认恢复后删除实验对象：

```powershell
kubectl --context docker-desktop -n kafka delete networkchaos kafka-0-network-delay
```

隔离实验对应的对象名为 `kafka-0-network-partition`。容器被杀死无法通过暂停“撤销”，需要等待 kubelet 重启和 Kafka 恢复；完成后删除 `podchaos kafka-0-container-kill`。

一次性实验结束后不会周期性重跑。再次测试时，先确认恢复并删除旧实验，再 apply 对应文件。若删除卡住，应查看控制器和 Daemon 日志确认清理失败原因，不要直接移除 finalizer。

## 6. 安装维护与兼容性

安装独立于 Kafka 应用和 Git 分支。切换代码分支不会卸载集群中的 Chaos Mesh，不需要重复安装。安装配置在 `helm-values.yaml`，登录权限配置在 `dashboard-rbac.yaml`。

Chart 固定缓存于 `chaos-mesh/.helm-cache/2.8.4/chaos-mesh`，已被 `.gitignore` 忽略。需要重建安装时，缓存存在就直接使用；不存在才下载：

```powershell
if (-not (Test-Path chaos-mesh/.helm-cache/2.8.4/chaos-mesh/Chart.yaml)) {
    helm pull https://charts.chaos-mesh.org/chaos-mesh-2.8.4.tgz --untar --untardir chaos-mesh/.helm-cache/2.8.4
}
helm upgrade --install chaos-mesh chaos-mesh/.helm-cache/2.8.4/chaos-mesh --kube-context docker-desktop --namespace chaos-mesh --create-namespace --values chaos-mesh/helm-values.yaml --wait --timeout 10m
kubectl --context docker-desktop apply -f chaos-mesh/dashboard-rbac.yaml
```

日常查看安装状态：

```powershell
helm --kube-context docker-desktop status chaos-mesh -n chaos-mesh
kubectl --context docker-desktop -n chaos-mesh get pods,pvc,svc
kubectl --context docker-desktop -n kafka get podchaos,networkchaos,schedules,workflows
```

当前安装使用 Docker socket `/var/run/docker.sock`。集群中可能已有历史实验对象；先检查其状态，再运行新的实验。不要将 `Running` 的 Kafka Pod 或已经恢复的实验对象视为业务正常的充分证据。

此前 Daemon 启动日志出现过 `grant access to /dev/fuse: fail to find device cgroup`，但 Daemon 继续启动并就绪。**IOChaos 可用性未验证**；故障注入能力应按具体实验结果判断，不应由组件 Ready 状态推断。

官方参考：[Helm 安装](https://chaos-mesh.org/docs/production-installation-using-helm/)、[权限管理](https://chaos-mesh.org/docs/manage-user-permissions/)、[实验运行与暂停](https://chaos-mesh.org/docs/run-a-chaos-experiment/)、[网络故障](https://chaos-mesh.org/docs/simulate-network-chaos-on-kubernetes/)。
