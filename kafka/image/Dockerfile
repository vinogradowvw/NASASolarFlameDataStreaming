FROM openjdk

RUN cd /opt && curl -OL https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz && tar -zxvf kafka_2.13-3.6.1.tgz && rm kafka_2.13-3.6.1.tgz
WORKDIR /opt/kafka_2.13-3.6.1

CMD bin/zookeeper-server-start.sh config/zookeeper.properties && \
    sleep 5 && \
    bin/kafka-server-start.sh config/server.properties
