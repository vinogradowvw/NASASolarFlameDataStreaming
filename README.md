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

#### Creating a topic 
```bash
$ docker exec -it kafka_kafka_1 kafka-topics.sh --create --bootstrap-server kafka:9092 --topic nasa-topic
```

Now we can access to the topic on port 9092 with name nasa-topic

![image](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/948a53e7-db40-43d4-aa96-4d875ea0ae2a)


# Power BI

To connect the cassandra to our Power BI we need a ODBC driver for cassandra, i use the Datastax one.

After installing we need to to set up the driver. For this step we need a ip of the cassandra's container (since we setted up the ports in docker-compose file also for the localhost, we can use simply 127.0.0.1:9042)

Then we need to go to the ODBC Data source administrator and find DataStax Cassandra ODBC DSN

After this steps we can see a ODBC for cassandra in Power BI.