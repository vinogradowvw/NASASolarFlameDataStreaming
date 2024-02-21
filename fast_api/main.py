import json
from fastapi import FastAPI
from data_parsing import get_notifications_data, get_solar_data
from aiokafka import AIOKafkaProducer
from datetime import datetime
from yaml import load, FullLoader

with open('../config.yml', 'r') as f:
    config = load(f, Loader=FullLoader)

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
    notification = await get_notifications_data()
    
    
    
    notification = json.dumps(notification).encode('utf-8')
    producer = await KafkaProducerSingleton.get_producer()
    await producer.send_and_wait(config['kafka']['TOPIC'], key='notification'.encode('utf-8'), value=notification)
    now = datetime.now()
    return {'message': 'Notification is sent to the topic at {}'.format(now)}


@app.post('/data')
async def root():
    solar_data = await get_solar_data()
    solar_data = json.dumps(solar_data).encode('utf-8')
    producer = await KafkaProducerSingleton.get_producer()
    await producer.send_and_wait(config['kafka']['TOPIC'], key='data'.encode('utf-8'), value=solar_data)
    now = datetime.now()
    return {'message': 'Data is sent to the topic at {}'.format(now)}
