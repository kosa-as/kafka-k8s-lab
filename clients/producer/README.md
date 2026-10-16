# 时间戳生产者

本模块向 `demo` topic 持续写入消息：key 为随机 UUID，value 为发送瞬间的 Unix epoch 毫秒时间戳字符串。Kafka Producer 使用 String 序列化、`acks=all` 和幂等生产。每次发送的分区、offset、key、value 或错误都会记录到文件。

在 `clients` 目录运行：

```powershell
mvn -pl producer compile exec:java -Dduration=60 -Dfrequency=10 -DbootstrapServers=localhost:32000 -Dtopic=demo
Get-Content .\logs\producer.log -Tail 20
```

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `duration` | `60` | 运行秒数，必须大于 0 |
| `frequency` | `1` | 每秒发送条数，必须大于 0 |
| `bootstrapServers` | `localhost:32000` | 宿主机 bootstrap 地址；集群内可改为 `bitnami-kafka.kafka.svc:9092` |
| `topic` | `demo` | 目标 topic |

这些是 Java 配置类的默认值。通过当前 Maven `exec:java` 运行时，请像上面的命令一样显式传入四个参数，避免未提供的 Maven 属性被传成空值。

Logback 只写入当前工作目录下的 `logs/producer.log`，不在控制台打印发送结果。从 `clients` 目录运行时，检查 `clients/logs/producer.log` 中的 `Message sent` 或 `Failed to send message`。日志达到 100 MB 后滚动，保留当前文件和最多 9 个归档文件。完整连接与测试步骤见[客户端总览](../README.md)。
