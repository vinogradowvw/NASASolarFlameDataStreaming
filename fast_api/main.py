import json
from fastapi import FastAPI
from data_parsing import get_notifications_data, get_solar_data
from aiokafka import AIOKafkaProducer
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from yaml import load, FullLoader

with open('../config.yml', 'r') as f:
    config = load(f, Loader=FullLoader)

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(config['cassandra']['IP'], auth_provider=auth_provider)

app = FastAPI()

class KafkaProducerSingleton:
    _producer = None
    
    @classmethod
    async def get_producer(cls):
        
        if cls._producer is None:
            cls._producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
            await cls._producer.start()
        return cls._producer
    

@app.post('/notifications')
async def root():
    try:
        notification = await get_notifications_data()
    except Exception as e:
        return {'message': f'Exceprtion while getting datafrom API: {e}'}
    try:
        session = cluster.connect()
    except Exception as e:
        return {'message': f'Exceprtion while creating a session: {e}'}
    message_ids = session.execute(query='''
                    USE solar_data;
                    SELECT DISTINCT(message_id) from solar_data;
                    ''')
    message_ids = list(message_ids)
    session.shutdown()
    
    if notification not in message_ids:
        notification = json.dumps(notification).encode('utf-8')
        producer = await KafkaProducerSingleton.get_producer()
        await producer.send_and_wait(config['kafka']['TOPIC'], key='notification'.encode('utf-8'), value=notification)
        now = datetime.now()
        return {'message': f'--------------New notification is sent to the topic at {now}--------------'}
    else:
        return {'message': '--------------Notification is alrady noted--------------'}


@app.post('/data')
async def root():
    solar_data = await get_solar_data()
    solar_data = json.dumps(solar_data).encode('utf-8')
    producer = await KafkaProducerSingleton.get_producer()
    await producer.send_and_wait(config['kafka']['TOPIC'], key='data'.encode('utf-8'), value=solar_data)
    now = datetime.now()
    return {'message': 'Data is sent to the topic at {}'.format(now)}
