## **Setting up the Kafka with Docker**

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

#### Creating the topic with partitions=2, so we will sent the data and notifications separately
```bash
$ docker exec -it kafka_kafka_1 kafka-topics.sh --create --bootstrap-server kafka:9092 --topic solar-data-topic --partitions 2 --replication-factor 1
```

Now we can access topic on port 9092

## **Setting up the Saprk with Docker**

docker-compose.yml:
```docker-compose.yml
version: '2'

services:
  spark:
    image: docker.io/bitnami/spark:3.5
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark
    ports:
      - '8080:8080'
    volumes:
      - ".:/files:rw"
  spark-worker:
    image: docker.io/bitnami/spark:3.5
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark
```

#### Creating actual docker container
```bash
$ docker-compose up -d
```

Now we can acces our Spark container with container id
```bash
$ docker exec -i -t 694a36d7b402 /bin/bash
```
