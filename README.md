## Containers
- `docker run --name zookeeper -p 2181:2181 zookeeper`
- `docker run --name kafka -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=192.168.0.33:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.0.33:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka`