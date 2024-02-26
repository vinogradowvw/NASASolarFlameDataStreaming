## **About**

I made this project to practice in my data engineering skills and learn new technologies such as Kafka, Spark, Fast API and Cassandra.
The data i used in this project goes from NASA API. There is 2 types of data: notifications about new solar flares and data about each flare.
I implemented "internal API" for getting the relevant data from NASA with FastAPI, afterwards, the data is sent to the Kafka, the spark reads the data and transforms it in the proper way and sending it to rhe  


## **Setting up the Kafka Docker image**

Using next Docker file and commands:


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
