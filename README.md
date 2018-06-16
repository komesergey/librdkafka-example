### Test project for librdkafka

### How to use:

1. Build and install [librdkafka](https://github.com/edenhill/librdkafka)
2. Start **ZooKeeper** and **Kafka** clusters
3. Create topic for test with command:
```commandline
kafka-topics.sh --create --zookeeper localhost:2181,localhost:2182,localhost:2183 --replication-factor 3 --partitions 1 --topic telemetry-replicated
```

You can test result using command:

```commandline
kafka-topics.sh --describe --zookeeper localhost:2181,localhost:2182,localhost:2183  --topic telemetry-replicated
```

4. Start **Producer** and **Consumer**
5. Enjoy.

##### Zookeeper and Topic settings in KafkaConsumer.cpp
```cmake
std::string brokers = "localhost:9091,localhost:9092,localhost:9093";
std::string topic = "telemetry-replicated";
```

##### Zookeeper and Topic settings in KafkaProducer.cpp
```cmake
std::string brokers = "127.0.0.1:9091, 127.0.0.1:9092, 127.0.0.1:9093";
std::string topic_str = "telemetry-replicated";
```