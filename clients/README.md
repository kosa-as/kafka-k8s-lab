# Kafka Producer and Consumer

两个独立的 Java Maven 命令行程序，连接当前工作区的 Strimzi Kafka。

## 当前集群配置

- Topic: demo
- 集群内地址: kafka-kafka-bootstrap.kafka.svc:9092
- Kubernetes bootstrap Service 的 NodePort 为 32325；Windows 客户端通过端口转发使用 localhost:40000。
- 三个 Broker 的本机转发地址分别为 localhost:40001、localhost:40002、localhost:40003。

## 运行

请在 \`clients\` 目录下执行以下命令。先在单独的 PowerShell 窗口启动 Kafka 本机端口转发:
```shell
..\strimzi\start-port-forwards.ps1
```

再启动生产者: 
```shell
mvn -pl producer compile exec:java -Dduration=600 -Dfrequency=100 -DbootstrapServers=localhost:40000 -Dtopic=demo
```
duration 单位为秒，frequency 单位为条每秒；生产者 payload 是发送瞬间的 Unix epoch 毫秒时间戳。

生产者和消费者日志使用 SLF4J + Logback，分别写入工作目录下的 `logs/producer.log` 和 `logs/consumer.log`。日志按文件大小滚动：每个文件最大 100MB，每类日志最多保留当前文件加 9 个归档文件（共 10 个）。

集群内部运行时，将 bootstrapServers 改为 kafka-kafka-bootstrap.kafka.svc:9092。


再启动消费者:
```shell
mvn -pl consumer compile exec:java -Dduration=600 -DbootstrapServers=localhost:40000 -Dtopic=demo
```
