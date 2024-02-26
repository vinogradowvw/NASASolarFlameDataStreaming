from aiokafka import AIOKafkaConsumer
import asyncio
import asyncio
import httpx
from yaml import load, FullLoader

with open('config.yml', 'r') as f:
    config = load(f, Loader=FullLoader)

# creating a loop where the data will be sent to the topic every minute
async def send_periodic_post_request():
    while True:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post('http://127.0.0.1:8000/notifications')
                if response.status_code == 200:
                    print(response.text)
                else:
                    print('/notifications: Message from FastAPI: ', response.status_code, response.text)
        except httpx.ReadTimeout as e:
            print(f"HTTP request timed out: {e}")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post('http://127.0.0.1:8000/solar_data')
                if response.status_code == 200:
                    print(response.text)
                else: 
                    print('/data ', response.status_code, response.text)
        except httpx.ReadTimeout as e:
            print("HTTP request timed out: {}".format(e))
        
        await asyncio.sleep(60)

# just to test lets make a consumer here
# async def consume_messages():
#     consumer = AIOKafkaConsumer(
#         config['kafka']['TOPIC'],
#         bootstrap_servers=config['kafka']['BOOTSTRAP_SERVER_EXTERNAL'],
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         value_deserializer=lambda x: x.decode('utf-8')
#     )

#     await consumer.start()

#     try:
#         while True:
#             async for message in consumer:
#                 # Handle the received message data
#                 print("Received message:")
#                 print(f"Topic: {message.topic}")
#                 print(f"Partition: {message.partition}")
#                 print(f"Offset: {message.offset}")
#                 print(f"Key: {message.key}")
#                 print(f"Value: {message.value}")
#     finally:
#         await consumer.stop()
    
asyncio.run(send_periodic_post_request())
