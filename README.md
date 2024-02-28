## **About**

The goal of this project is to practice in my data engineering skills and learn new technologies such as Kafka, Spark, Fast API and Cassandra.
The data i used in this project goes from NASA API. There is 2 types of data: notifications about new solar flares and data about each flare.
I implemented "internal API" for getting the relevant data from NASA with FastAPI, afterwards, the data is sent to the Kafka, the spark reads the data and transforms it in the proper way and sending it to cassandra and in the end Power BI takes data from cassandra and visualizing it.

You can see how the architecure of the solution looks like:

![Arch](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/a9e61fed-01a8-45b7-8cdb-410dc59a41ec)

## **Docker**
The first step in this project is to set up all of the parts in the docker containers.
In the [docker-compose.yaml](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/blob/main/docker-compose.yml) file you can find a set up that worked for me
#### Containers, UP!
```bash
$ docker-compose up -d
```

## **FastAPI + Kafka**
Firstly, we need to create a topic in the kafka brocker. 
#### Creating a topic
```bash
$ docker exec -it ebd59f311d27 /bin/bash
$ kafka-topics.sh --create --bootstrap-server kafka:9092 --topic solar-data-topic --partitions 2 --replication-factor 1
```
![image](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/45487e9b-cc21-4f2f-a028-a63f0bb97a8b)

So now, we can access to the topic from localhost at the port 9092 or also from the docker network.

Now, it is FastAPI's turn.
So what actually FastAPI is doing. There is 2 post edpoints - 1 for the data about the solar flares and 2 for the notifications (alerts)
[fast_api](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/tree/main/fast_api) directory is the directory where the fast api is written
In the file [kafka_stream.py](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/blob/main/kafka/kafka_stream.py) decribed only repeating post requests of our API, imitating actual real-time data flow.

## **Cassandra**
All of the CQL scrips for cassandra keyspace and tables setup are in the python script [cassandra_setting.py](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/blob/main/cassandra/cassandra_setting.py)
You can just run as the normal mython scrip and it will execute the CQL scripts inside your docker container.

## **Spark**
I used spark as a real time transforming and processing tool. In the spark i made those calculations: Pulling out the summary for the alert, converting classtype metric to the numerical equivalent and calculated duration of the solar flare.

The [jobs](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/tree/main/jobs) directory is mounted with the spark docker container, so i can run this job straight forward from the container.

```bash
docker exec -it 88913e5c39f2 spark-submit --master spark://172.23.0.2:7077 --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0","com.datastax.spark:spark-cassandra-connector_2.12:3.2.0","com.github.jnr:jnr-posix:3.1.15" --conf spark.cassandra.connection.host=084f2e99ef2a --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=cassandra jobs/spark_streaming.py --executor-memory 2g --driver-memory 2g
```

## **Power BI**

To connect the cassandra to our Power BI we need a ODBC driver for cassandra, i use the Datastax one.

After installing we need to to set up the driver. For this step we need a ip of the cassandra's container (since we setted up the ports in docker-compose file also for the localhost, we can use simply 127.0.0.1:9042)

 - Then we need to go to the ODBC Data source administrator and find DataStax Cassandra ODBC DSN
![Screenshot (4)](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/aee1ae90-6c56-484f-ad1d-181bc4bc1cf1)

 - After this steps we can see a ODBC for cassandra in Power BI.
![Screenshot 2024-02-25 124132](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/590a65c3-1c4d-4d77-b6c0-ae76f9a50ea0)

 - Finally, after we have all set up, we can make a dashboard that visualize our data:
![image](https://github.com/vinogradowvw/NASASolarFlameDataStreaming/assets/143388794/fad85e8d-a6e8-4750-91fa-7d124853593c)

