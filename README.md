## **Setting up the Kafka Docker image**

Using next Docker file and commands:

```docker-compose.yml
version: "3"
services:
  zookeeper:
    image: 'bitnami/zookeeper:latest'
    ports:
      - '2181:2181'
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
  kafka:
    image: 'bitnami/kafka:latest'
    ports:
      - '9092:9092'
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_LISTENERS=PLAINTEXT://:9092
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
    depends_on:
      - zookeeper
```

#### Creating actual docker containers (Zookeeper and Kafka)
```bash
$ docker-compose up -d
```

#### Creating a separate topics for sending the notifications and data
```bash
$ docker exec -it kafka_kafka_1 kafka-topics.sh --create --bootstrap-server kafka:9092 --topic solar-data-topic
$ docker exec -it kafka_kafka_1 kafka-topics.sh --create --bootstrap-server kafka:9092 --topic notifications-topic
```

Now we can access topics on port 9092

![image](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/e3d8f33f-d7f9-4aaa-bf68-69839365afe5)

