# Java 生产者与消费者

`producer/` 和 `consumer/` 是两个独立的 Maven 模块，分别向 Kafka 写入时间戳和按消费组读取消息。代码使用 Java 17 编译，依赖 `kafka-clients` 4.3.1；当前实验 broker 为 Kafka 4.0.0，已完成实际生产、消费验证。运行前请安装 JDK 17 或更新版本及 Maven，并确认 [Kafka 集群](../README.md)已就绪。

## 连接方式

| 运行位置 | `bootstrapServers` |
| --- | --- |
| Docker Desktop 宿主机 | `localhost:32000` |
| Kubernetes 集群内 | `bitnami-kafka.kafka.svc:9092` |

宿主机客户端取得元数据后还会连接 `localhost:32001` 至 `localhost:32003`，因此四个 NodePort 都需要可用。当前部署不需要 `kubectl port-forward`。默认 topic 为 `demo`，其三个分区和副本由部署脚本创建。

## 构建与运行

以下命令从仓库根目录开始。生产者和消费者可在两个 PowerShell 窗口同时运行；第二个窗口也要先进入 `clients` 目录。

```powershell
Set-Location .\clients
mvn test
mvn -pl producer compile exec:java -Dduration=60 -Dfrequency=2 -DbootstrapServers=localhost:32000 -Dtopic=demo
mvn -pl consumer compile exec:java -Dduration=60 -DbootstrapServers=localhost:32000 -Dtopic=demo -DgroupId=demo-consumer
```

在 IntelliJ IDEA 的 Maven 运行配置中，工作目录设为 `clients`，并把对应命令从 `-pl` 起填入 Goals；不要给 `-D` 参数加单引号。单引号在该运行配置中会原样传给 `mvn.cmd`，使 Maven 把参数误判为插件目标。

`duration` 单位为秒，`frequency` 单位为每秒发送的消息数。生产者将随机 UUID 用作 key，将发送时的 Unix epoch 毫秒时间戳用作 value，启用 `acks=all` 和幂等生产。消费者订阅 `demo`，使用 String 反序列化和自动提交 offset；对没有已提交 offset 的新消费组，从最早可用消息开始读取。

## 参数与默认值

| 参数 | 生产者默认值 | 消费者默认值 | 说明 |
| --- | --- | --- | --- |
| `duration` | `60` | `60` | 运行秒数，必须大于 0 |
| `frequency` | `1` | 不适用 | 每秒消息数，必须大于 0 |
| `bootstrapServers` | `localhost:32000` | `localhost:32000` | Kafka bootstrap 地址 |
| `topic` | `demo` | `demo` | 读写的 topic |
| `groupId` | 不适用 | `timestamp-consumer` | 消费组；换新组可从最早可用消息读取 |

表中的默认值来自 Java 配置类。当前 Maven `exec:java` 配置会将未提供的属性传入程序，可能使程序收到空值；使用上述 Maven 命令时，请显式填写对应模块的全部参数。

## 查看日志与排查

Logback 只配置了文件输出，控制台上的 Maven 成功信息不代表消息发送或消费成功。从 `clients` 目录运行时，程序日志写入 `clients/logs/producer.log` 和 `clients/logs/consumer.log`；每个文件达到 100 MB 后滚动，最多保留当前文件和 9 个归档文件。

```powershell
Get-Content .\logs\producer.log -Tail 20 -Wait
Get-Content .\logs\consumer.log -Tail 20 -Wait
```

生产者日志应出现 `Message sent`，消费者日志应出现 `Message received`。若消费者没有新消息，先确认生产者已写入，再为消费者指定一个新的 `groupId`；已有消费组可能已提交到末尾。连接超时时检查 Kafka Pod、`demo` topic 和 `32000` 至 `32003` 端口。

模块细节见[生产者](producer/README.md)与[消费者](consumer/README.md)。
