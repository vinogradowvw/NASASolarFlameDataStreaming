import asyncio
from datetime import datetime
from pyspark.sql.functions import col, from_json, last
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.accumulators import AccumulatorParam

notification_schema = StructType([
    StructField('messageType', StringType(), False),
    StructField('messageID', StringType(), False),
    StructField('messageURL', StringType(), False),
    StructField('messageIssueTime', StringType(), False),
    StructField('messageBody', StringType(), False)
])

def process_solar_data(raw_data):
    solar_data = {
        'duration' : [],
        'class_type_value' : [],
        'time_stamp' : []
    }

    def parse_class_type(classType: str) -> float:
        class_map = {
            'A' : 10,
            'B' : 20,
            'C' : 30,
            'M' : 40,
            'X' : 50
        }
        return class_map[classType[0]] + float(classType[1:])
        
    for solar_flare in raw_data:
        solar_data['class_type_value'].append(parse_class_type(solar_flare['classType']))
        duration = datetime.strptime(solar_flare['endTime'], '%Y-%m-%dT%H:%MZ') - datetime.strptime(solar_flare['beginTime'], '%Y-%m-%dT%H:%MZ')
        duration = duration.seconds
        solar_data['duration'].append(
            duration
        )
        solar_data['time_stamp'].append(solar_flare['peakTime'])
    
    return solar_data

def process_notification(raw_notification):
    notification = raw_notification
    message_start = raw_notification['messageBody'].find('## Summary:') + 11
    message_end = raw_notification['messageBody'].find('## Notes:')
    notification['messageBody'] = raw_notification['messageBody'][message_start:message_end]
    return notification


class StingAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return value
    def addInPlace(self, val1, val2):
        return val2
    

def read_kafka_topic():
    
    global notification_schema
    
    def spark_session() -> SparkSession:
        conf = SparkConf()
        conf.setAll(
            [
                ("spark.master", 'spark://172.23.0.2:7077'),
                ('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'),
            ]
        )
        return SparkSession.builder.config(conf=conf).getOrCreate()

    spark = spark_session()
    sc = spark.sparkContext
    
    streaming_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'kafka:29092') \
        .option('subscribe', 'solar-data-topic') \
        .option('startingOffsets', 'earliest') \
        .load()

    notification_df = streaming_df.selectExpr("cast(value as string) as value") \
        .withColumn("value", from_json(col('value'), notification_schema)) \
        .select("value.*") \
        .selectExpr('messageID', 'messageURL', 'cast(messageIssueTime as timestamp) as messageIssueTime', 'messageBody')
    
    query = notification_df.writeStream.outputMode("append").format("console").option("truncate", "false").start()
    query.awaitTermination()


read_kafka_topic()



# await consumer.start()




# 
# print(va.value)
        
# Поток Spark Streaming для обработки данных
# kafka_stream = ssc.receiverStream(KafkaReceiver(consumer))
# kafka_stream.foreachRDD(lambda rdd: rdd.foreach(process_message))

# ssc.start()
# ssc.awaitTermination()