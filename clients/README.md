# Kafka Producer and Consumer

两个独立的 Java Maven 命令行程序，连接当前工作区的 Bitnami Kafka。

## 当前集群配置

- Topic: demo
- 集群内地址: bitnami-kafka.kafka.svc:9092
- Windows bootstrap NodePort: localhost:32000；Broker 的 advertised 地址为 localhost:32001、localhost:32002、localhost:32003。
- 不需要启动 `kubectl port-forward`。

## 运行

请在 `clients` 目录下执行。启动生产者：
```shell
mvn -pl producer compile exec:java -Dduration=600 -Dfrequency=100 -DbootstrapServers=localhost:32000 -Dtopic=demo
```
duration 单位为秒，frequency 单位为条每秒；生产者 payload 是发送瞬间的 Unix epoch 毫秒时间戳。

生产者和消费者日志使用 SLF4J + Logback，分别写入工作目录下的 `logs/producer.log` 和 `logs/consumer.log`。日志按文件大小滚动：每个文件最大 100MB，每类日志最多保留当前文件加 9 个归档文件（共 10 个）。

集群内部运行时，将 bootstrapServers 改为 bitnami-kafka.kafka.svc:9092。


再启动消费者:
```shell
mvn -pl consumer compile exec:java -Dduration=600 -DbootstrapServers=localhost:32000 -Dtopic=demo
```
