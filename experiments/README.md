# 第五章 Kafka 实机验证

`validate_health.py` 在实际 Kafka 和 Kubernetes 上采集试次，再从原始事件离线复算。共用的运行入口接收 M0 或 M1，当前已在 M0 原生探针环境试跑。M0 会检查目标实际使用 Controller TCP Readiness 和 `pgrep -f kafka` Liveness。后续可通过相同参数指向 M1 的已部署 Pod。

## 构建与运行

在工作树根目录执行，要求 Python 3.10+、Java 17+、Maven 和可读取目标 Pod 及 Kubelet `/metrics/probes` 的 kubectl。默认连接 Docker Desktop 上的 Kafka 和 `localhost:32000`。

```powershell
mvn -B -f clients/pom.xml -pl producer test org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath '-Dmdep.outputFile=target/validation-classpath.txt'
python experiments/validate_health.py run --group M0 --scenario healthy --output .planning/health-runs
python experiments/validate_health.py run --group M0 --scenario min-isr --output .planning/health-runs
python experiments/validate_health.py summarize .planning/health-runs/<run-id>
python -m unittest discover -s experiments
```

两个预实验默认各建一个唯一的 `health-validation-*` Topic，分区 0、三个副本、健康场景 minISR=2、ISR 场景 minISR=3、记录保留一小时。Topic 保留用于复查。健康试验也可用 `--topic` 指定允许写入的现有 Topic，并用 `--partition` 固定分区。自动目标 Pod 名称遵循本项目 Bitnami 命名，指定自定义 Pod 时同时提供 `--pod` 和 `--broker-id`，以校验该 Broker 当前确为 Leader。

`min-isr` 仅适用于本项目本机默认集群，需要 Chaos Mesh 的 NetworkChaos。在 RF=3、minISR=3 的独立 Topic 以两条/秒持续写入，用有时限的 NetworkChaos 隔离目标 Leader 和选定的一个 follower 之间的双向 Pod 链路。由于 Pod 同时承担 Broker 与 Controller，隔离可能影响该链路上的其他分区和控制面。实际目标 Leader 未变且 ISR 低于 3 时才认定故障成立。随后删除本试次创建的注入，继续采样到目标 ISR 恢复。Kafka 生效阈值按配置值与副本数取小。正式比较若要求沿用论文的 minISR=2，需要独立验证在该配置下触发 ISR<2 的场景。

## 记录与口径

- `injection.json` 与 `injection-status.json` 保存实机网络注入清单和 Chaos Mesh 确认状态。
- `run.json` 保存目标、组别、Pod UID、初始 Kafka 容器代际、探针配置、时间窗口、代码提交和实际文件哈希。
- `events.jsonl` 保存本机 UTC 毫秒时间和试次编号，包含负载发送、确认、消费、独立 Admin 采集的 Leader、ISR 和 minISR、注入动作、目标 Pod 采样和 Kubelet 探针计数。两个 Kafka 客户端采集进程的诊断日志保存在各自 stderr 文件中。
- 独立 Admin 通过并行请求取得分区状态和配置，保存采集起止区间。故障窗口以连续的目标 Leader 和低于 minISR 证据为准。注入未产生目标条件与采集缺测各有结果标记。
- 探针事件根据计数前后两次轮询定位。计数或 Pod 代际不连续时可能无法据此判断。Pod 就绪和重启通过定期采样验证，短于间隔的状态变化可能遗漏。M0/M1 正式比较前应确认该采集精度足够。
- `summary.json` 按发送时所在窗口统计生产确认。生产 P99 使用成功确认的单调时钟耗时。消费以成功确认的消息 ID 为分母，统计发送后五秒内的交付，并计算相应的 P99。负载在窗口结束后短暂继续，以便接住窗口尾部的确认和消费。
- 此处参数及实机结果属于预实验。共用事件和评分先在 M0 验收，M1 接入后用同一入口做正式配对。

## RQ3 同条件校准

在各组对应的工作树运行采集，先确认 Broker 2 实际部署的是该组。M1 采集要求 enforce。每对使用相同的 `--pair-id`、Broker、观察对端、窗口和负载。ISR 试次还需指定同一个可隔离 follower，且独立观察对端与当前 Controller 不得落在被隔离的一侧。以下以对端 Broker 1、可隔离 follower 0 为例，校准前核对实际拓扑。

```powershell
python experiments/validate_health.py run --group M0 --scenario healthy --pair-id H-01 --broker-id 2 --observer-port 32002 --healthy-seconds 60 --resource-seconds 5 --output .planning/calibration
python experiments/validate_health.py run --group M0 --scenario min-isr --pair-id I3-01 --broker-id 2 --observer-port 32002 --follower-id 0 --healthy-seconds 60 --fault-seconds 120 --recovery-seconds 120 --resource-seconds 5 --output .planning/calibration
python experiments/compare_health.py <M0试次目录> <M1试次目录> --output .planning/calibration/pair.json
```

在 M1 工作树用相同参数执行 `--group M1`。配对脚本直接复算两份事件，核对采集源码散列、负载、目标、对端和探针周期阈值。资源统计使用 Kubelet 的 `stats/summary`，分别给出每个容器的 CPU 核数及内存工作集样本数和中位数。重复返回的 Kubelet 时间戳只算一次。汇总同时记录迟到消费、确认后未观测到的消息，以及未获生产确认却已观测到的消息。

冷启动用 `python experiments/cold_start.py --group M0 --pair-id B-01 --output .planning/calibration/start-M0`。在 M1 工作树用 M1 参数重复。该命令只重启 Kafka 容器，观察 Pod UID、容器代际、Controller 协议响应、Startup 状态和 Ready。通过 Pod 端口转发直接发送无记录的 Produce 请求，以独立于 Service 是否转发流量的方式测试 Produce 处理入口。该请求只验证协议处理，完整生产确认及消费仍由业务基准测量。

## 执行停滞与本地追加停滞

两个场景使用 Kafka 4.0.0 运行字节码中的明确处理边界。用独立试验 Topic 的分区 0，副本数 3、minISR=2，Leader 固定为临时取证的 Broker 2。Producer 每四秒发送，关闭单次发送的自动重试。Agent 只在 Broker 主进程启动，并按 Topic、客户端和消息标记限定选中请求。

| 场景 | 注入位置 | 成立与恢复判据 |
|---|---|---|
| `execution-stall` | `KafkaApis.handleProduceRequest` 的执行入口 | 目标消息到达 Produce 处理入口并暂停约 16 秒。暂停解除后，同一 ID 才进入并完成本地追加。 |
| `storage-stall` | `LocalLog.append` 的本地写入入口 | 同一 ID 已通过 Produce 处理入口，进入本地追加后暂停约 16 秒，解除后记录追加返回。另有目标请求在这段时间到达 Produce 处理入口，支持执行路径仍在推进。 |

每轮保留 `broker-stage.log`，并用同一消息 ID 关联客户端超时、Broker 分阶段时间与恢复。ISR 采样必须保持三个副本，原生探针和 Pod 由共用入口记录。实验模拟的是单条请求在指定位置停止推进。存储场景的边界是本地日志追加调用，物理介质、刷盘及 Broker 全部请求线程的停滞需另设实验。

### 临时取证部署

用 Broker 镜像的 Kafka 4.0.0 JAR 编译 agent。将 `kafka_2.13-4.0.0.jar`、`kafka-clients-4.0.0.jar`、`kafka-storage-4.0.0.jar`、`kafka-server-4.0.0.jar` 和 `scala-library` JAR 从当前运行的 Kafka 容器中复制到本地 `$brokerLibraries`。JAR 位于 `/opt/bitnami/kafka/libs/`。构建脚本使用 Maven 缓存的 ASM 9.8，也可用 `-AsmJar` 指定同版本文件。

```powershell
./experiments/stall-agent/build.ps1 -BrokerLibraries $brokerLibraries -OutputJar $agentJar
python experiments/m0_stall_deployment.py install $stateDir --agent $agentJar
try {
    python experiments/validate_health.py run --group M0 --scenario execution-stall --healthy-seconds 22 --fault-seconds 30 --recovery-seconds 22 --output $results
    if ($LASTEXITCODE -ne 0) { throw 'Execution stall trial failed.' }
    python experiments/validate_health.py run --group M0 --scenario storage-stall --healthy-seconds 22 --fault-seconds 30 --recovery-seconds 22 --output $results
} finally {
    python experiments/m0_stall_deployment.py restore $stateDir
}
```

`$stateDir` 使用新的本地路径，保存原始探针配置及回滚所需资源信息。安装和恢复各滚动替换一次最高序号 Kafka Pod。试次之间等待自然恢复，末尾核对原生探针、Kafka 节点就绪和集群 ISR。
