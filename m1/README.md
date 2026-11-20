# Kafka M1 健康检测原型

原型在本项目的 Bitnami Kafka 4.0.0 部署上监测 Broker 2 的指定 Topic 分区 0。平时使用专用的 `health-validation-m1-b2`。验证程序在试次期间切换至实验 Topic，结束时恢复。副本数 3，正常目标分区首选 Leader 为 2。Pod 内的常驻 Sidecar 每轮取得前后两次 Admin 元数据，向本地 Leader 定向发送限速诊断 Produce，读取独立 Broker 阶段观测并输出状态。诊断使用 acks=1、禁用重试，正常时约每 1.1 秒一条；实际轮次还包含元数据调用时间。业务客户端使用原有的 acks=all 配置。

## 观测与判定

- Kafka 4.0.0 的 `KafkaApis.handleProduceRequest` 入口与方法体内实际 `Request.body` 正常返回标记执行进展。`LocalLog.append` 的入口和成功返回标记本地追加。按本地目标分区、试验或诊断客户端以及记录的 36 字节关联键取证。常驻观测使用自身的阶段、线程和时间信息；故障标签仅供实验评分。
- 追加仅纳入已观测到执行进展的同一条记录。任一仍处在该阶段且超过两秒的调用优先产生有效失败。对逾期调用核实相应工作线程仍在目标方法内，发现证据丢失时给对应项一个未知窗口。四千余条未清理的记录触发清理与证据失效处理。其余情况下按自上轮以来新增的进展或追加证据判断。
- Broker 每 250 毫秒原子发布 `observation`；代际、目标、序号、心跳及分项有效位约束可用性。代际错误或超过 1.5 秒的阶段证据不能支持有效失败。ISR 判据为本地 Leader 且当前 ISR 小于配置 minISR 和副本数的较小值。元数据过期、冲突或缺失按 UNKNOWN 处理；确认为非本地 Leader 时 ISR 项通过，追加项未知。
- `Decision.java` 实现第四章的 N=2、R=2。每项的缺失和故障分别计数，执行有效成功才能独立确认或解除存储故障。任一项持续失败或缺失撤销就绪。连续两轮完整健康恢复就绪。Sidecar 将计数、代际、采集证据和轮次原子写入 `decision`，同代际重启可从该文件恢复。
- `probe.sh` 在 Kafka 容器中执行。Startup 仍检查 9093。Readiness 要求容器内的 9093 可达、同代际同目标快照未超过 8 秒、已启动且就绪。Liveness 要求 Kafka 进程存在且同代际没有已确认执行失败。容器内 TCP 和原生 Kubelet→Pod 的 9093 TCP 路径在网络隔离场景中可能不同。原有周期、超时和失败阈值仍由 Kubelet 执行。

## 构建、部署及回滚

在增强工作树根目录使用 Docker Desktop 的 Kafka、kubectl、Java 17+、Maven 和 ASM 9.8。若本地 Maven 库缺少 ASM，可用 Maven 下载 `org.ow2.asm:asm:9.8`。默认构建脚本从正在运行的 Kafka 容器读取对应版本的五个 API 和 Scala JAR，也可用 `-BrokerLibraries` 指定已提取目录。

```powershell
$d = Join-Path (Get-Location) '.planning/m1-build'
New-Item -ItemType Directory -Force -Path $d | Out-Null
$jar = Join-Path $d 'm1.jar'
./m1/build.ps1 -OutputJar $jar
$state = Join-Path $d 'deployment'
python m1/deploy.py install $state --jar $jar
python m1/deploy.py enforce $state
```

`install` 要求新的状态目录，只替换 StatefulSet 的最高序号 Pod，保存原始字段和探针，先以 shadow 模式运行。安装完成或移除验证组件后，脚本会核实专用诊断分区在 Broker 2 且三个副本在线。启用 enforce 前需有完整健康判定。正式监测可用 `kubectl -n kafka logs bitnami-kafka-controller-2 -c health-sidecar` 查看 M1_ROUND，Kafka 容器中的 `/var/run/kafka-health/{epoch,observation,decision}` 提供当前快照。

在当前局部部署下，`python m1/deploy.py shadow $state` 可恢复只记录模式；`python m1/deploy.py restore $state` 恢复原始 StatefulSet 并清除 M1 ConfigMap。另一部署入口为固定 Chart 的 `deploy/kafka.ps1 -M1`，它在已有程序 ConfigMap 的前提下加载 `m1/helm-values.json`，适合用 Helm 持久化管理增强配置。采用 Helm 管理后的回退应使用同一 Helm 部署历史。

## 实验与证据

执行和存储试次另需既有的 M0 实验 agent。以 `install ... --fixture <stall.jar>` 装入同一注入逻辑，保留独立的 M1 观测。使用 `experiments/validate_health.py run --group M1` 运行 `execution-stall`、`storage-stall`、`min-isr`。实验选择期间目标文件使用互斥目录保护，M1 诊断键和验证端业务 UUID 使用不同标识。确定故障已经解除后，`python m1/deploy.py remove-fixture $state` 将滚动替换目标 Pod、恢复 enforce 并清除注入 agent 的二进制数据。

原始文件包括客户端/平台 `events.jsonl`、真值 `broker-stage.log` 或 Chaos Mesh 状态，以及 M1 自身的 `m1-rounds.log`。用 `experiments/validate_health.py summarize <试次目录>` 复算。对单条消息约 16 秒的局部停滞，需区分 M1 内部确认、探针单次失败与最终平台门槛。正式 RQ3 统计还需要同条件的重复配对与资源开销测量。
