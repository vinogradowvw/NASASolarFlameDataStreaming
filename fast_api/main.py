from enum import Enum
import json
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from data_parsing import get_notifications_data, get_solar_data
from aiokafka import AIOKafkaProducer
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from yaml import load, FullLoader

with open('../config.yml', 'r') as f:
    config = load(f, Loader=FullLoader)

app = FastAPI()


@app.post('/notifications')
async def send_notification():
    try:
        notification = await get_notifications_data()
    except Exception as e:
        return JSONResponse(status_code=502, content={'error': "/notifications: Can not get data from API {}".format(e)})
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster([config['cassandra']['IP']], auth_provider=auth_provider)
        session = cluster.connect()
    except Exception as e:
        return JSONResponse(status_code=502, content={'error': "/notifications: Exceprtion while creating a session: {}".format(e)})

    session.execute('USE nasa_project;')
    query='SELECT DISTINCT CAST(message_id AS TEXT) from notifications;'
    message_ids = [row.message_id for row in session.execute(query=query)]
    session.shutdown()
    
    if notification['messageID'] not in message_ids and not notification['messageID'] == None:
        notification = json.dumps(notification).encode('utf-8')
        try:
            producer = AIOKafkaProducer(bootstrap_servers=config['kafka']['BOOTSTRAP_SERVER_EXTERNAL'])
            await producer.start()
            await producer.send_and_wait(config['kafka']['TOPIC'], key='notifications'.encode('utf-8'), value=notification)
            now = datetime.now()
            return '------New notification is sent to the topic at {} ------'.format(now)
        except Exception as e:
            return JSONResponse(status_code=502, content={'error': "/notifications: Can not connect to kafka: {}".format(e)})
        finally:
            await producer.stop()
    else:
        return JSONResponse(status_code=206, content={'error': "/notifications: Notification was already noted"})

@app.post('/solar_data')
async def send_solar_data():
    try:
        solar_data = await get_solar_data()
    except Exception as e:
        return JSONResponse(status_code=502, content={'error': "/solar_data: Can not get data from API {}".format(e)})
    
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster([config['cassandra']['IP']], auth_provider=auth_provider)
        session = cluster.connect()
    except Exception as e:
        return JSONResponse(status_code=502, content={'error': "/solar_data: Exceprtion while creating a session: {}".format(e)})
    
    session.execute('USE nasa_project;')
    query='SELECT DISTINCT CAST(flr_id AS TEXT) from solar_data;'
    flr_ids = [row.flr_id for row in session.execute(query=query)]
    session.shutdown()
    producer = AIOKafkaProducer(bootstrap_servers=config['kafka']['BOOTSTRAP_SERVER_EXTERNAL'])
    try:
        await producer.start()
        count = 0
        for row in solar_data:
            if row['flrID'] is not None and row['flrID'] not in flr_ids:
                count += 1
                row_json = json.dumps(row).encode('utf-8')
                await producer.send_and_wait(config['kafka']['TOPIC'], key='data'.encode('utf-8'), value=row_json)
                
        now = datetime.now()
        return '-----New {} rows of solar data are sent to the topic at {}-----'.format(count, now)
    except Exception as e:
        return JSONResponse(status_code=502, content={'error': "/solar_data: Can not connect to Kafka: {}".format(e)})
    finally:
        await producer.stop()