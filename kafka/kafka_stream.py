from aiokafka import AIOKafkaConsumer
import asyncio
import asyncio
import httpx
from yaml import load, FullLoader

with open('config.yml', 'r') as f:
    config = load(f, Loader=FullLoader)

# creating a loop where the data will be sent to the topic every 10 seconds
async def send_periodic_post_request():
    while True:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post('http://127.0.0.1:8000/notifications')
                if response.status_code != 200:
                    print('/notifications: Error on FastAPI side: ', response.status_code)
        except httpx.ReadTimeout as e:
            print(f"HTTP request timed out: {e}")
        await asyncio.sleep(10)

# just to test lets make a consumer here
async def consume_messages():
    consumer = AIOKafkaConsumer(
        config['kafka']['TOPIC'],
        bootstrap_servers=config['kafka']['BOOTSTRAP_SERVER_INTERNAL'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    await consumer.start()

    try:
        while True:
            async for message in consumer:
                # Handle the received message data
                print("Received message:")
                print(f"Topic: {message.topic}")
                print(f"Partition: {message.partition}")
                print(f"Offset: {message.offset}")
                print(f"Key: {message.key}")
                print(f"Value: {message.value}")
    finally:
        await consumer.stop()


async def main():
    tasks = [send_periodic_post_request(), consume_messages()]
    await asyncio.gather(*tasks)
    
asyncio.run(send_periodic_post_request())
