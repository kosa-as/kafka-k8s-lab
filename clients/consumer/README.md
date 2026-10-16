# 时间戳消费者

本模块订阅 `demo` topic，读取生产者写入的 UUID key 和毫秒时间戳 value。程序按消费组运行，使用 String 反序列化、自动提交 offset；新消费组在没有已提交 offset 时从最早可用消息开始。

在 `clients` 目录运行：

```powershell
mvn -pl consumer compile exec:java -Dduration=60 -DbootstrapServers=localhost:32000 -Dtopic=demo -DgroupId=demo-consumer
Get-Content .\logs\consumer.log -Tail 20
```

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `duration` | `60` | 运行秒数，必须大于 0 |
| `bootstrapServers` | `localhost:32000` | 宿主机 bootstrap 地址；集群内可改为 `bitnami-kafka.kafka.svc:9092` |
| `topic` | `demo` | 订阅的 topic；留空时回退为默认值 |
| `groupId` | `timestamp-consumer` | 消费组；留空时回退为默认值 |

这些是 Java 配置类的默认值。通过当前 Maven `exec:java` 运行时，请像上面的命令一样显式传入 `duration`、`bootstrapServers`、`topic` 和 `groupId`，避免未提供的 Maven 属性被传成空值。

Logback 只写入当前工作目录下的 `logs/consumer.log`，不在控制台打印消息。从 `clients` 目录运行时，查看 `clients/logs/consumer.log` 中的 `Message received`，其中包含 partition、offset、key 和 value。日志达到 100 MB 后滚动，保留当前文件和最多 9 个归档文件。

如果日志中没有消息，先确认生产者已写入，再使用新的 `groupId`。同一消费组可能已提交到 topic 末尾。完整连接与测试步骤见[客户端总览](../README.md)。
