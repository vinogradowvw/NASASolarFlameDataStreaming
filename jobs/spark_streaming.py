from pyspark.sql.functions import col, from_json, udf, unix_timestamp, regexp_extract
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, IntegerType

notification_schema = StructType([
    StructField('messageType', StringType(), False),
    StructField('messageID', StringType(), False),
    StructField('messageURL', StringType(), False),
    StructField('messageIssueTime', StringType(), False),
    StructField('messageBody', StringType(), False)
])

data_schema = StructType([
    StructField('flrID', StringType(), False),
    StructField('instruments', ArrayType(StringType()), False),
    StructField('beginTime', StringType(), False),
    StructField('peakTime', StringType(), False),
    StructField('endTime', StringType(), False),
    StructField('classType', StringType(), False),
    StructField('sourceLocation', StringType(), False),
    StructField('activeRegionNum', IntegerType(), False),
    StructField('linkedEvents', ArrayType(MapType(StringType(), StringType())), False),
    StructField('link', StringType(), False)
])


def parse_class_type(classType):
    class_map = {
        'A' : 10,
        'B' : 20,
        'C' : 30,
        'M' : 40,
        'X' : 50
    }
    return class_map[classType[0]] + float(classType[1:])

parse_class_type_udf = udf(parse_class_type)

def get_summary_message(message_body):
    message_start = message_body.find('## Summary:') + 11
    message_end = message_body.find('## Notes:')
    return message_body[message_start:message_end]

get_summary_message_udf = udf(get_summary_message)

def read_kafka_topic():
    
    global notification_schema
    
    def spark_session() -> SparkSession:
        conf = SparkConf()
        conf.setAll(
            [
                ("spark.master", 'spark://172.23.0.4:7077'),
                ('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'),
                ('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1'),
                ('spark.jars.packages', 'com.github.jnr:jnr-posix:3.1.15'),
                ('spark.cossandra.connection.host', '084f2e99ef2a'),
                ("spark.cassandra.auth.username", "cassandra"),
                ("spark.cassandra.auth.password", "cassandra"),
                ("spark.scheduler.mode", "FAIR")
            ]
        )
        return SparkSession.builder.config(conf=conf).getOrCreate()

    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
        
    streaming_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'kafka:29092') \
        .option('subscribe', 'solar-data-topic') \
        .option('startingOffsets', 'earliest') \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    notification_df = streaming_df.filter(col("key") == "notifications")
    
    data_df = streaming_df.filter(col("key") == "data")
    
    data_df = data_df \
        .withColumn("value", from_json(col('value'), data_schema)) \
        .select("value.*") \
        .selectExpr('cast(flrID as string) as flr_id', 
                    'cast(beginTime as string) as begin_time', 
                    'cast(endTime as string) as end_time', 
                    'cast(peakTime as string) as peak_time',
                    'cast(classType as string) as class_type',
                    'cast(sourceLocation as string) as source_location',
                    'cast(activeRegionNum as int) as active_region_num',
                    'cast(link as string) as link') \
        .withColumn('duration', (unix_timestamp(col("end_time"), "yyyy-MM-dd'T'HH:mm'Z'") - unix_timestamp(col("begin_time"), "yyyy-MM-dd'T'HH:mm'Z'"))) \
        .withColumn('class_letter', regexp_extract(col('class_type'), '([A-Za-z]+)', 1)) \
        .withColumn('class_type_encoded', parse_class_type_udf(col('class_type')))

    notification_df = notification_df.selectExpr("cast(value as string) as value") \
        .withColumn("value", from_json(col('value'), notification_schema)) \
        .select("value.*") \
        .selectExpr('cast(messageID as string) as message_id', 
                    'cast(messageURL as string) as message_url', 
                    'cast(messageIssueTime as string) as message_issue_time', 
                    'cast(messageBody as string) as message_body') \
        .withColumn("message_body", get_summary_message_udf(col("message_body")))
    
    query_data = data_df.writeStream.format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "nasa_project") \
        .option("table", "solar_data") \
        .outputMode('append') \
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/data") \
        .start()
    
    query_notification = notification_df.writeStream.format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "nasa_project") \
        .option("table", "notifications") \
        .outputMode('append') \
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/notifications") \
        .start()
    
    query_notification.awaitTermination()


read_kafka_topic()
