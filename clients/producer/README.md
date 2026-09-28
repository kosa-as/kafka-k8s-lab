# Producer

发送当前 epoch 毫秒时间戳。执行: 
```shell
mvn compile exec:java -Dduration=60 -Dfrequency=10 -DbootstrapServers=localhost:40000 -Dtopic=demo
```

duration 为秒，frequency 为条每秒；默认值分别为 60、1，默认地址为 localhost:40000。

日志写入工作目录下的 `logs/producer.log`。
