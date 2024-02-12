## **Setting up the Kafka Docker image**

Using next Docker file and commands:

```Dockerfile
FROM openjdk

RUN cd /opt && curl -OL https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz && tar -zxvf kafka_2.13-3.6.1.tgz && rm kafka_2.13-3.6.1.tgz
WORKDIR /opt/kafka_2.13-3.6.1

CMD bin/zookeeper-server-start.sh config/zookeeper.properties && \
    sleep 5 && \
    bin/kafka-server-start.sh config/server.properties
```

```bash
$ sudo docker build . -t kafka:2.13-3.6.1
$ sudo docker run -d --name kafka-broker kafka:2.13-3.6.1
```

![image](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/db6ec9d3-ced1-4f0a-911e-1c26b521452e)
