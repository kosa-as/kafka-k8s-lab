# Consumer

消费 demo topic，执行: mvn compile exec:java -Dduration=60 -DbootstrapServers=localhost:40000 -Dtopic=demo -DgroupId=timestamp-consumer

duration 单位为秒，默认 60；程序打印 partition、offset 和时间戳 payload。

日志写入工作目录下的 `logs/consumer.log`。
